import pytest

from utils.resources.database import DatabaseResource


class FakeDatabaseCall:
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
def database_call():
    return FakeDatabaseCall()


@pytest.fixture
def make_resource(database_call):
    def _make_resource(**options):
        return DatabaseResource(
            object="students",
            call=database_call,
            **options,
        )

    return _make_resource


def test_initialization(make_resource, database_call):
    resource = make_resource()

    assert resource.object == "students"
    assert resource.call is database_call
    assert resource.write_disposition == "append"
    assert resource.incremental_attribute is None
    assert resource.incremental_obj is None
    assert resource.schema is None


def test_incremental_initialization(make_resource):
    resource = make_resource(
        incremental_attribute="updated",
        incremental_initial_value=100,
    )

    assert resource.incremental_attribute == "updated"
    assert resource.incremental_obj is not None
    assert resource.incremental_obj.cursor_path == "updated"


def test_incremental_value_is_passed_to_database_call(make_resource, database_call):
    database_call.records = [{"id": 2, "updated": 200}]
    resource = make_resource(incremental_attribute="updated")
    incremental_obj = FakeIncremental(last_value=100)

    results = list(resource.yield_query_results(incremental_obj))

    assert results == [{"id": 2, "updated": 200}]
    assert database_call.received_args == [
        {
            "object": "students",
            "incremental": {
                "attribute": "updated",
                "value": 100,
            },
        }
    ]


@pytest.mark.parametrize("last_value", [None, 0])
def test_falsy_incremental_value_is_passed_as_none(
    make_resource,
    database_call,
    last_value,
):
    resource = make_resource(incremental_attribute="updated")
    incremental_obj = FakeIncremental(last_value=last_value)

    list(resource.yield_query_results(incremental_obj))

    assert database_call.received_args[0]["incremental"]["value"] is None


def test_no_incremental_attribute(make_resource, database_call):
    resource = make_resource()

    list(resource.yield_query_results())

    assert database_call.received_args == [
        {
            "object": "students",
            "incremental": {
                "attribute": None,
                "value": None,
            },
        }
    ]


def test_schema_is_passed_to_database_call(make_resource, database_call):
    resource = make_resource(schema="public")

    list(resource.yield_query_results())

    assert database_call.received_args == [
        {
            "object": "students",
            "incremental": {
                "attribute": None,
                "value": None,
            },
            "schema": "public",
        }
    ]


def test_schema_is_omitted_when_not_configured(make_resource, database_call):
    resource = make_resource()

    list(resource.yield_query_results())

    assert "schema" not in database_call.received_args[0]


def test_create_resource_yields_database_records(make_resource, database_call):
    database_call.records = [
        {"id": 1, "name": "Ada"},
        {"id": 2, "name": "Grace"},
    ]
    dlt_resource = make_resource().create_resource()

    assert list(dlt_resource) == [
        {"id": 1, "name": "Ada"},
        {"id": 2, "name": "Grace"},
    ]


def test_table_name_includes_prefix_and_suffix(make_resource):
    resource = make_resource(
        table_name_prefix="source_",
        table_name_suffix="_raw",
    )

    assert resource.create_resource().name == "source_students_raw"


def test_create_resource_uses_write_disposition(make_resource):
    resource = make_resource(write_disposition="replace")

    assert resource.create_resource().write_disposition == "replace"