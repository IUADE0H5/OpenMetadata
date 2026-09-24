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
"""Per-source progress state, composed into a source rather than mixed in.

A source *has a* ``ProgressTracking`` object (reached through the
``progress_tracking`` property its base defines) instead of *being* a progress
mixin. The object owns the registry, the declared mode, and the guarded manual
facade; ``attach_progress_tracking`` builds it lazily so a step that never
tracks progress has no registry and the workflow reporter (which scans steps
for the ``_progress_tracking`` attribute) skips it.
"""

from typing import Any, Optional

from metadata.ingestion.progress.modes import (
    ManualProgress,
    ProgressMode,
    ProgressModeError,
)
from metadata.ingestion.progress.registry import ProgressRegistry


class ProgressTracking:
    """One source's progress state: the registry, the declared mode, and the
    guarded manual facade. Composed into a source, not inherited."""

    def __init__(self, mode: ProgressMode, source_name: str) -> None:
        self._mode = mode
        self._source_name = source_name
        self._registry = ProgressRegistry()
        self._manual: Optional[ManualProgress] = None  # noqa: UP045

    @property
    def mode(self) -> ProgressMode:
        return self._mode

    @property
    def registry(self) -> ProgressRegistry:
        return self._registry

    @property
    def manual(self) -> ManualProgress:
        """MANUAL-mode counting facade. Raises for AUTO/OFF sources — the
        runner counts AUTO sources, so a manual increment would double-count."""
        if self._mode is not ProgressMode.MANUAL:
            raise ProgressModeError(
                f"progress_tracking.manual requires progress_mode=MANUAL, but {self._source_name} "
                f"declares {self._mode.name}. AUTO sources are counted by the topology runner "
                "and may only declare totals via declare_progress_totals()."
            )
        if self._manual is None:
            self._manual = ManualProgress(self._registry)
        return self._manual


def share_progress_tracking(source: Any, *steps: Any) -> None:
    """Give processor/stage/sink steps the source's ``ProgressTracking`` so a
    multi-step workflow (usage: read, parse, stage, publish) reports every phase
    on the one progress line the workflow heartbeat renders. The workflow reads
    the registry off the first step that has one, which stays the source."""
    tracking = attach_progress_tracking(source)
    for step in steps:
        step.__dict__["_progress_tracking"] = tracking


def shared_progress(step: Any) -> Optional[ManualProgress]:  # noqa: UP045
    """The manual progress facade a non-source step was handed by
    ``share_progress_tracking``, or ``None`` when the step runs on its own (unit
    tests, a workflow that did not share) - callers guard on it."""
    tracking = step.__dict__.get("_progress_tracking")
    if tracking is None or tracking.mode is not ProgressMode.MANUAL:
        return None
    return tracking.manual


def attach_progress_tracking(source: Any) -> ProgressTracking:
    """Lazily build and cache the source's ``ProgressTracking``. Kept a
    module-level helper (not a base class) so any host composes progress
    without inheriting a progress mixin. First access is single-threaded (in
    ``_iter``, before worker threads spawn), so the lazy init is safe."""
    tracking = source.__dict__.get("_progress_tracking")
    if tracking is None:
        mode = getattr(source, "progress_mode", ProgressMode.AUTO)
        tracking = ProgressTracking(mode, type(source).__name__)
        source.__dict__["_progress_tracking"] = tracking
    return tracking
