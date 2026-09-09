#  Copyright 2025 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
Delete methods
"""

import traceback
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set, Type  # noqa: UP035

from metadata.config.settings import ingestion_settings
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.ingestion.api.models import Either
from metadata.ingestion.models.barrier import Barrier
from metadata.ingestion.models.delete_entity import DeleteEntity
from metadata.ingestion.ometa.ometa_api import OpenMetadata, T
from metadata.utils.fqn import quote_name
from metadata.utils.logger import utils_logger

logger = utils_logger()

SERVICE_SCOPE_KEY = "service"


@dataclass
class StaleDeleteGuard:
    """What a run may empty. Stale deletion removes every in-scope entity the run did not
    produce, so a run that produced nothing for a scope it lists - a filter that matches nothing
    in the source, a source that failed for that scope - would wipe the scope. Unless told it may,
    such a scope is left untouched and reported.

    One guard is shared by all the scopes of a run: with ``allow_empty_scope`` a single scope may
    be emptied per run, and only ``allow_multiple_empty_scopes`` restores the unrestricted
    behaviour. Scopes the run saw at least one entity in are reconciled normally.
    """

    allow_empty_scope: bool = False
    allow_multiple_empty_scopes: bool = False
    emptied: List[str] = field(default_factory=list)  # noqa: UP006

    def permits_emptying(self, scope_fqn: str) -> Optional[str]:  # noqa: UP045
        """None when the scope may be emptied, else the flag that would allow it."""
        if not self.allow_empty_scope:
            return "allowEmptyingSchema"
        if self.emptied and scope_fqn not in self.emptied and not self.allow_multiple_empty_scopes:
            return "allowEmptyingMultipleSchemas"
        if scope_fqn not in self.emptied:
            self.emptied.append(scope_fqn)
        return None


def _seen_in_scope(entity_source_state: Set[str], scope_fqn: str) -> bool:  # noqa: UP006
    prefix = scope_fqn + "."
    return any(fqn.startswith(prefix) for fqn in entity_source_state)


def _live_in_scope(metadata: OpenMetadata, entity_type: Type[T], params: Dict[str, str]) -> int:  # noqa: UP006
    listing = metadata.list_entities(entity=entity_type, params=params, limit=1)
    return int(getattr(listing, "total", 0) or 0)


def _default_dispatch_async() -> bool:
    return ingestion_settings.delete_async


def _scope_params_as_fqn(params: Optional[Dict[str, str]]) -> Optional[Dict[str, str]]:  # noqa: UP006, UP045
    """
    Return ``params`` with the service scope expressed as an FQN instead of a raw name.

    ``deleteStale`` resolves the scope against the stored FQN, which for a service is its quoted
    name - so a service whose name needs quoting (in practice, one containing a dot) never
    resolves and nothing is ever deleted. The legacy fallback still needs the raw name, since
    ``?service=`` matches a service by name, so the original dict is left untouched.

    Only the service scope is converted: every other scope key already arrives as a full FQN, and
    quoting a multi-part FQN would break it. Quoting is idempotent, so a caller that already
    passes an FQN is unaffected. Key order is preserved because the scope is read from the first
    entry.
    """
    if not params or SERVICE_SCOPE_KEY not in params:
        return params
    service = params[SERVICE_SCOPE_KEY]
    try:
        return {**params, SERVICE_SCOPE_KEY: quote_name(service)}
    except ValueError:
        logger.warning("Cannot build the FQN of service '%s'; sending the delete scope unchanged", service)
        return params


PROCEED, SKIP = "proceed", "skip"


def _guard_decision(
    metadata: OpenMetadata,
    entity_type: Type[T],  # noqa: UP006
    entity_source_state: Set[str],  # noqa: UP006
    params: Dict[str, str],  # noqa: UP006
    guard: StaleDeleteGuard,
):
    """``PROCEED`` (something was seen in the scope, or the guard allows emptying it), ``SKIP``
    (the scope holds nothing live, so there is nothing to delete), or the ``StackTraceError`` to
    report instead of emptying the scope."""
    scope_fqn = next(iter(params.values()))
    if _seen_in_scope(entity_source_state, scope_fqn):
        return PROCEED
    live = _live_in_scope(metadata, entity_type, params)
    if live == 0:
        return SKIP
    needed = guard.permits_emptying(scope_fqn)
    if needed is None:
        return PROCEED
    return StackTraceError(
        name=f"Delete stale {entity_type.__name__} in {scope_fqn}",
        error=(
            f"Refused: this run produced no {entity_type.__name__} for {scope_fqn} but it holds "
            f"{live} - deleting them would empty the scope. Check the filters and the source; set "
            f"{needed} on the pipeline if that is intended."
        ),
        stackTrace=None,
    )


def delete_entity_from_source(
    metadata: OpenMetadata,
    entity_type: Type[T],  # noqa: UP006
    entity_source_state: Set[str],  # noqa: UP006
    recursive: bool = True,
    params: Optional[Dict[str, str]] = None,  # noqa: UP006, UP045
    dispatch_async: Optional[bool] = None,  # noqa: UP045
    guard: Optional[StaleDeleteGuard] = None,  # noqa: UP045
) -> Iterable[Either[DeleteEntity]]:
    """
    Soft-delete the entities of ``entity_type`` within ``params`` scope that were not seen in
    this run. The server owns the detection: the connector sends the set of FQNs it produced
    (``entity_source_state``) and the server soft-deletes the in-scope entities not in that set.

    Whether stale deletion runs at all is decided by the caller (the ``markDeleted*`` source
    config gate); this function only performs it.

    Falls back to the legacy client-side paginate-and-diff against older servers that do not
    expose the ``deleteStale`` endpoint.

    :param metadata: OMeta client
    :param entity_type: Pydantic Entity model
    :param entity_source_state: FQNs of the entities produced by the connector this run
    :param recursive: When True, the soft-delete cascades to child entities
    :param params: single-key scope dict, e.g. {"database": fqn} / {"databaseSchema": fqn}. A
        {"service": name} scope may be passed as the raw service name; it is converted to the
        service FQN for the server call — see :func:`_scope_params_as_fqn`.
    :param dispatch_async: For the legacy fallback path, route the sink delete through the
        server-side async endpoint (returns 202 + jobId, runs cascade on the server's
        executor) so ingestion does not block on large hierarchies — see issue #4003. The
        server-side bulk deleteStale path is already async by design and ignores this flag.
    :param guard: When given, a scope the run produced nothing for is not emptied unless the
        guard allows it; the refusal is reported as a failure of the run. Without it the scope is
        emptied, as before.
    """
    use_async = dispatch_async if dispatch_async is not None else _default_dispatch_async()
    if guard is not None and params:
        decision = _guard_decision(metadata, entity_type, entity_source_state, params, guard)
        if decision == SKIP:
            return
        if decision != PROCEED:
            logger.warning(f"{decision.name}: {decision.error}")
            yield Either(left=decision)  # pyright: ignore[reportCallIssue]
            return
    # Flush the sink buffer so the scope entity and the entities seen this run are committed
    # before the server resolves the scope and computes what is stale.
    barrier = Barrier(reason=f"flush_before_delete_stale:{entity_type.__name__}")
    yield Either(right=barrier)  # pyright: ignore[reportCallIssue]
    try:
        result = metadata.delete_stale_entities(
            entity=entity_type,
            scope_params=_scope_params_as_fqn(params),
            live_fqns=entity_source_state,
            recursive=recursive,
        )
        if result is not None:
            # The server soft-deleted the stale entities; nothing to push through the sink.
            return
        # Older server without the deleteStale endpoint: fall back to client-side detection.
        yield from _delete_stale_entities_legacy(
            metadata,
            entity_type,
            entity_source_state,
            recursive,
            params,
            use_async,
        )
    except Exception as exc:
        yield Either(  # pyright: ignore[reportCallIssue]
            left=StackTraceError(
                name="Delete Entity",
                error=f"Error deleting {entity_type.__class__}: {exc}",
                stackTrace=traceback.format_exc(),
            )
        )


def _delete_stale_entities_legacy(
    metadata: OpenMetadata,
    entity_type: Type[T],  # noqa: UP006
    entity_source_state: Set[str],  # noqa: UP006
    recursive: bool,
    params: Optional[Dict[str, str]],  # noqa: UP006, UP045
    dispatch_async: bool,
) -> Iterable[Either[DeleteEntity]]:
    """Legacy client-side stale detection: paginate the scope and diff FQNs locally."""
    entity_state = metadata.list_all_entities(entity=entity_type, params=params)
    for entity in entity_state:
        if str(entity.fullyQualifiedName.root) not in entity_source_state:
            yield Either(
                left=None,
                right=DeleteEntity(
                    entity=entity,
                    recursive=recursive,
                    dispatch_async=dispatch_async,
                ),
            )


def delete_entity_by_name(
    metadata: OpenMetadata,
    entity_type: Type[T],  # noqa: UP006
    entity_names: List[str],  # noqa: UP006
    recursive: bool = True,
    dispatch_async: Optional[bool] = None,  # noqa: UP045
) -> Iterable[Either[DeleteEntity]]:
    """
    Method to delete the entities contained on a given list
    :param metadata: OMeta client
    :param entity_type: Pydantic Entity model
    :param entity_names: List of FullyQualifiedNames of the entities to be deleted
    :param recursive: When True, the delete cascades to child entities
    :param dispatch_async: see :func:`delete_entity_from_source`
    """
    use_async = dispatch_async if dispatch_async is not None else _default_dispatch_async()
    try:
        for entity_name in entity_names:
            entity = metadata.get_by_name(entity=entity_type, fqn=entity_name)
            if entity:
                yield Either(
                    left=None,
                    right=DeleteEntity(
                        entity=entity,
                        recursive=recursive,
                        dispatch_async=use_async,
                    ),
                )
    except Exception as exc:
        yield Either(  # pyright: ignore[reportCallIssue]
            left=StackTraceError(
                name="Delete Entity",
                error=f"Error deleting {entity_type.__class__}: {exc}",
                stackTrace=traceback.format_exc(),
            )
        )
