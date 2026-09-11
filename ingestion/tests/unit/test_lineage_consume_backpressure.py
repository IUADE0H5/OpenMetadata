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
Throughput and memory safety of lineage consumption:
- generate_lineage_with_processes applies backpressure so a fast producer cannot grow the
  inter-stage queue without bound (D)
- IngestionWorkflow._consume_concurrent drains the source across threads, losing nothing (C)
"""

import threading
import time
from unittest.mock import MagicMock

from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import Sink
from metadata.ingestion.source.database.lineage_source import LineageSource
from metadata.workflow.ingestion import IngestionWorkflow


class TestGenerateLineageBackpressure:
    """The producer must stop racing ahead of a slow consumer."""

    def test_queue_depth_stays_bounded(self):
        chunk_size, max_threads, max_queued, total = 10, 2, 50, 600
        produced = {"n": 0}
        produced_lock = threading.Lock()

        def producer_fn():
            yield from range(total)

        def processor_fn(chunk, queue, *_):
            for item in chunk:
                queue.put(Either(right=item))
                with produced_lock:
                    produced["n"] += 1

        gen = LineageSource.generate_lineage_with_processes(
            producer_fn,
            processor_fn,
            args=(),
            chunk_size=chunk_size,
            max_threads=max_threads,
            max_queued=max_queued,
        )

        consumed = 0
        peak_backlog = 0
        for _ in gen:
            consumed += 1
            with produced_lock:
                backlog = produced["n"] - consumed
            peak_backlog = max(peak_backlog, backlog)
            time.sleep(0.001)  # a slow sink: force the producer to wait on the cap

        assert consumed == total  # nothing dropped by the throttle
        # Overshoot past the cap is bounded by what the in-flight chunks can add after the gate check.
        assert peak_backlog <= max_queued + chunk_size * max_threads + chunk_size


class _RecordingSink(Sink):
    """Sink that records which thread handled each record and sleeps to force overlap."""

    def __init__(self):
        super().__init__()
        self.threads: set = set()
        self.handled: list = []
        self._lock = threading.Lock()

    @classmethod
    def create(cls, config_dict, metadata, pipeline_name=None):
        return cls()

    def _run(self, record, *_, **__) -> Either:
        time.sleep(0.005)
        with self._lock:
            self.threads.add(threading.current_thread().name)
            self.handled.append(record)
        return Either(right=record)

    def close(self) -> None:
        pass


class _FakeSource:
    def __init__(self, records):
        self._records = records

    def run(self):
        yield from self._records


class _BareWorkflow(IngestionWorkflow):
    """Concrete IngestionWorkflow so the consume helpers can be tested without a real service."""

    def set_steps(self):  # abstract in the base; unused by the consume-loop tests
        pass


class TestConsumeConcurrent:
    """_consume_concurrent delivers every record and actually uses multiple threads."""

    def _workflow(self, source, steps):
        wf = _BareWorkflow.__new__(_BareWorkflow)
        wf.source = source
        wf.steps = steps
        wf.metadata = MagicMock()
        return wf

    def test_all_records_delivered_across_threads(self):
        records = list(range(200))
        sink = _RecordingSink()
        wf = self._workflow(_FakeSource(records), [sink])

        wf._consume_concurrent(workers=8)

        assert sorted(sink.handled) == records  # every record processed exactly once
        assert len(sink.threads) > 1  # genuinely parallel, not silently serial

    def test_grows_http_pool_to_the_run_fan_out(self):
        """The consumer sizes the client pool to consumer + producer-resolve concurrency so
        connections are reused, not opened-and-discarded."""
        sink = _RecordingSink()
        wf = self._workflow(_FakeSource(range(10)), [sink])

        wf._consume_concurrent(workers=8)

        wf.metadata.client.ensure_pool_maxsize.assert_called_once_with(80)

    def test_serial_path_used_when_single_worker(self):
        records = list(range(20))
        sink = _RecordingSink()
        wf = self._workflow(_FakeSource(records), [sink])

        wf._consume_serial()

        assert sorted(sink.handled) == records
        assert len(sink.threads) == 1  # one consumer thread on the serial path


class TestConsumerWorkers:
    """Only query-log lineage with threads>1 opts into concurrent consume."""

    def _workflow_with_source_config(self, source_config):
        wf = _BareWorkflow.__new__(_BareWorkflow)

        class _Cfg:
            pass

        cfg = _Cfg()
        cfg.source = _Cfg()
        cfg.source.sourceConfig = _Cfg()
        cfg.source.sourceConfig.config = source_config
        wf.config = cfg
        return wf

    def test_lineage_threads_drive_worker_count(self):
        from metadata.generated.schema.metadataIngestion.databaseServiceQueryLineagePipeline import (
            DatabaseServiceQueryLineagePipeline,
        )

        wf = self._workflow_with_source_config(DatabaseServiceQueryLineagePipeline(threads=8))
        assert wf._consumer_workers() == 8

    def test_non_lineage_pipeline_stays_serial(self):
        from metadata.generated.schema.metadataIngestion.databaseServiceMetadataPipeline import (
            DatabaseServiceMetadataPipeline,
        )

        wf = self._workflow_with_source_config(DatabaseServiceMetadataPipeline())
        assert wf._consumer_workers() == 1

    def test_lineage_without_threads_stays_serial(self):
        from metadata.generated.schema.metadataIngestion.databaseServiceQueryLineagePipeline import (
            DatabaseServiceQueryLineagePipeline,
        )

        wf = self._workflow_with_source_config(DatabaseServiceQueryLineagePipeline(threads=1))
        assert wf._consumer_workers() == 1
