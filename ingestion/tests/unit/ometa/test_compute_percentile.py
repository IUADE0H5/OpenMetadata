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
"""compute.percentile is one heavy synchronous server job: never retried, a gateway timeout is
reported as started-not-confirmed, any other error still raises."""

import logging
from unittest.mock import MagicMock

import pytest

from metadata.generated.schema.entity.data.table import Table
from metadata.ingestion.ometa.client import APIError
from metadata.ingestion.ometa.ometa_api import OpenMetadata


def _api(post):
    api = OpenMetadata.__new__(OpenMetadata)
    api.client = MagicMock()
    api.client.post = post
    return api


def _error(status):
    http_error = MagicMock()
    http_error.response.status_code = status
    return APIError({"code": status, "message": "gateway"}, http_error)


def test_no_retries_and_a_gateway_timeout_is_a_warning_not_an_error(caplog):
    post = MagicMock(side_effect=_error(504))
    with caplog.at_level(logging.WARNING):
        assert OpenMetadata.compute_percentile(_api(post), Table, "2026-09-14") is False
    assert post.call_count == 1 and post.call_args.kwargs["retries"] == 0
    assert "the server keeps computing" in caplog.text


def test_a_server_error_still_raises():
    with pytest.raises(APIError):
        OpenMetadata.compute_percentile(_api(MagicMock(side_effect=_error(500))), Table, "2026-09-14")


def test_success_is_confirmed():
    assert OpenMetadata.compute_percentile(_api(MagicMock(return_value={})), Table, "2026-09-14") is True
