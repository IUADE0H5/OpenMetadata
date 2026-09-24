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
"""A multi-phase run declares one counter per phase; the ETA follows the phase in progress and is
measured from that counter's own first increment, not from the start of the run."""

from unittest.mock import patch

from metadata.ingestion.progress.registry import ProgressRegistry


def test_eta_follows_the_counter_that_is_moving_and_uses_its_own_clock():
    registry = ProgressRegistry()
    with patch("metadata.ingestion.progress.registry.time.monotonic", return_value=0.0):
        registry.set_total("Queries", 100)
        registry.set_total("Statements", 50)
        registry.set_total("Usage records", 10)
    # before anything moves: the last declared counter is the driver (the old rule), no ETA yet
    assert registry._driver_counter()[0] == "Usage records"
    assert registry.eta_seconds() is None

    with patch("metadata.ingestion.progress.registry.time.monotonic", return_value=100.0):
        registry.track("Queries", 25)  # the read phase starts at t=100
    with patch("metadata.ingestion.progress.registry.time.monotonic", return_value=110.0):
        # 25 of 100 in 10s of *this* phase -> 30s left, not the 330s a run-elapsed rate would give
        assert registry._driver_counter()[0] == "Queries"
        assert registry.eta_seconds() == 30

    with patch("metadata.ingestion.progress.registry.time.monotonic", return_value=120.0):
        registry.track("Queries", 75)  # read phase complete
        registry.track("Statements", 10)  # parse phase starts at t=120
    with patch("metadata.ingestion.progress.registry.time.monotonic", return_value=125.0):
        assert registry._driver_counter()[0] == "Statements"
        assert registry.eta_seconds() == 20  # 10 of 50 in 5s -> 20s

    with patch("metadata.ingestion.progress.registry.time.monotonic", return_value=130.0):
        registry.track("Statements", 40)
    # every started counter complete, the not-yet-started last one is the driver again, no ETA
    assert registry._driver_counter()[0] == "Usage records"
    assert registry.eta_seconds() is None
