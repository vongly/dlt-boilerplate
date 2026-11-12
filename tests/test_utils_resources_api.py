import pytest

from utils.resources.api import APIResource


class FakeAPICall:
    def __init__(self, records=None):
        self.records = records if records is not None else []
        self.received_args = []

    def yield_records(self, **kwargs):
        self.received_args.append(kwargs)
        yield from self.records


class FakeIncremental:
    def __init__(self, last_value=None):
        self.last_value = last_value


@pytest.fixture
def api_call():
    return FakeAPICall()


@pytest.fixture
def make_resource(api_call):
    def _make_resource(**options):
        return APIResource(
            object="customers",
            call=api_call,
            **options,
        )

    return _make_resource


def test_initialization(make_resource, api_call):
    resource = make_resource()

    assert resource.object == "customers"
    assert resource.call is api_call
    assert resource.write_disposition == "append"
    assert resource.incremental_attribute is None
    assert resource.incremental_obj is None


def test_incremental_initialization(make_resource):
    resource = make_resource(
        incremental_attribute="created",
        incremental_initial_value=100,
    )

    assert resource.incremental_attribute == "created"
    assert resource.incremental_obj is not None


def test_stripe_incremental_attribute_cleaning(make_resource):
    resource = make_resource(
        api_source="stripe",
        incremental_attribute="created[gte]",
    )

    assert resource.incremental_obj.cursor_path == "created"


def test_non_stripe_incremental_attribute_is_unchanged(make_resource):
    resource = make_resource(
        api_source="other",
        incremental_attribute="created[gte]",
    )

    assert resource.incremental_obj.cursor_path == "created[gte]"


def test_incremental_value_is_passed_to_api(make_resource, api_call):
    api_call.records = [{"id": "cus_123", "created": 200}]
    resource = make_resource(incremental_attribute="created")
    incremental_obj = FakeIncremental(last_value=200)

    results = list(resource.yield_query_results(incremental_obj))

    assert results == [{"id": "cus_123", "created": 200}]
    assert api_call.received_args == [
        {
            "object": "customers",
            "incremental": {
                "attribute": "created",
                "value": 200,
            },
        }
    ]


@pytest.mark.parametrize("last_value", [None, 0])
def test_falsy_incremental_value_is_passed_as_none(
    make_resource,
    api_call,
    last_value,
):
    resource = make_resource(incremental_attribute="created")
    incremental_obj = FakeIncremental(last_value=last_value)

    list(resource.yield_query_results(incremental_obj))

    assert api_call.received_args[0]["incremental"]["value"] is None


def test_no_incremental_attribute(make_resource, api_call):
    resource = make_resource()

    list(resource.yield_query_results())

    assert api_call.received_args == [
        {
            "object": "customers",
            "incremental": {
                "attribute": None,
                "value": None,
            },
        }
    ]


def test_create_resource_yields_api_records(make_resource, api_call):
    api_call.records = [
        {"id": "cus_123"},
        {"id": "cus_456"},
    ]
    dlt_resource = make_resource().create_resource()

    assert list(dlt_resource) == [
        {"id": "cus_123"},
        {"id": "cus_456"},
    ]


def test_table_name_includes_prefix_and_suffix(make_resource):
    resource = make_resource(
        table_name_prefix="stripe_",
        table_name_suffix="_raw",
    )

    assert resource.create_resource().name == "stripe_customers_raw"


def test_create_resource_uses_write_disposition(make_resource):
    resource = make_resource(write_disposition="replace")

    assert resource.create_resource().write_disposition == "replace"
