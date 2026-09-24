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
"""Processor, stage and sink steps count on the source's registry once the workflow shares it."""

from types import SimpleNamespace

from metadata.ingestion.progress.modes import ProgressMode
from metadata.ingestion.progress.tracking import share_progress_tracking, shared_progress


class _ManualSource:
    progress_mode = ProgressMode.MANUAL


class _AutoSource:
    progress_mode = ProgressMode.AUTO


def test_steps_share_the_manual_source_registry():
    source, processor, sink = _ManualSource(), SimpleNamespace(), SimpleNamespace()
    assert shared_progress(processor) is None  # a step on its own counts nothing and never fails

    share_progress_tracking(source, processor, sink)

    shared_progress(processor).seed_scope_total("Statements", "batch 1", 4)
    shared_progress(sink).track("Statements", 3)
    assert source._progress_tracking.registry.global_counters() == [("Statements", 3, 4)]
    assert processor._progress_tracking is source._progress_tracking


def test_an_auto_source_hands_its_steps_no_manual_facade():
    source, stage = _AutoSource(), SimpleNamespace()
    share_progress_tracking(source, stage)
    assert shared_progress(stage) is None  # the runner counts AUTO sources; a manual track would double-count
