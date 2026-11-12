import pytest

from datetime import datetime, timezone
from unittest.mock import Mock

from utils.connections.stripe import StripeCall


@pytest.fixture
def stripe_call():
    return StripeCall(
        api_secret="sk_test_fake",
        test=True,
        test_limit=2,
    )

def test_stripe_call_initialization(stripe_call):
    assert stripe_call.url is not None
    assert stripe_call.headers["Authorization"] == "Bearer sk_test_fake"
    assert stripe_call.test is True
    assert stripe_call.test_limit == 2

def test_yield_records(stripe_call, monkeypatch):
    fake_response = Mock()

    fake_response.status_code = 200
    fake_response.json.return_value = {
        "data": [
            {
                "id": "cus_001",
                "name": "Customer One",
            },
            {
                "id": "cus_002",
                "name": "Customer Two",
            },
        ],
        "has_more": False,
    }

    mock_get = Mock(return_value=fake_response)

    monkeypatch.setattr(
        "utils.connections.stripe.requests.get",
        mock_get,
    )

    records = list(
        stripe_call.yield_records(
            object="customers",
        )
    )

    assert len(records) == 2
    assert records[0]["id"] == "cus_001"
    assert records[1]["id"] == "cus_002"

    for record in records:
        assert "_dlt_processed" in record

    mock_get.assert_called_once()

    args, kwargs = mock_get.call_args

    assert args[0] == f"{stripe_call.url}/customers"
    assert kwargs["headers"] == stripe_call.headers

    assert kwargs["params"]["limit"] == 2


def test_yield_records_incremental(stripe_call, monkeypatch):
    fake_response = Mock()

    fake_response.status_code = 200
    fake_response.json.return_value = {
        "data": [
            {
                "id": "cus_003",
                "name": "Customer Three",
            },
        ],
        "has_more": False,
    }

    mock_get = Mock(return_value=fake_response)

    monkeypatch.setattr(
        "utils.connections.stripe.requests.get",
        mock_get,
    )

    records = list(
        stripe_call.yield_records(
            object="customers",
            incremental={
                "attribute": "created",
                "value": 100,
            },
        )
    )

    assert len(records) == 1

    _, kwargs = mock_get.call_args

    assert kwargs["params"]["created"] == 100


def test_yield_records_pagination(stripe_call, monkeypatch):
    first_response = Mock()

    first_response.status_code = 200
    first_response.json.return_value = {
        "data": [
            {"id": "cus_001"},
        ],
        "has_more": True,
    }

    second_response = Mock()
    second_response.status_code = 200
    second_response.json.return_value = {
        "data": [
            {"id": "cus_002"},
        ],
        "has_more": False,
    }

    mock_get = Mock(
        side_effect=[
            first_response,
            second_response,
        ]
    )

    monkeypatch.setattr(
        "utils.connections.stripe.requests.get",
        mock_get,
    )

    monkeypatch.setattr(
        "utils.connections.stripe.time.sleep",
        lambda _: None,
    )

    stripe_call.test = False

    records = list(
        stripe_call.yield_records(
            object="customers",
        )
    )

    assert len(records) == 2
    assert records[0]["id"] == "cus_001"
    assert records[1]["id"] == "cus_002"

    assert mock_get.call_count == 2

    second_call_kwargs = mock_get.call_args_list[1].kwargs

    assert second_call_kwargs["params"]["starting_after"] == "cus_001"
