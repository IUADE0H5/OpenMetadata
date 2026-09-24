"""The patch-edge warnings say what the server refused, not only the status code."""

from metadata.ingestion.ometa.client import APIError
from metadata.ingestion.ometa.mixins.lineage_mixin import _api_error_message


def test_the_servers_message_is_used_when_the_body_is_json():
    err = APIError({"code": 400, "message": "Invalid path /columnsLineage/41"})
    assert _api_error_message(err) == "Invalid path /columnsLineage/41"


def test_a_bodiless_error_still_says_something():
    err = APIError({"code": 400, "message": ""})  # what a JsonMappingException 400 arrives as
    assert "400" in _api_error_message(err)  # never an empty string
