import os
os.environ["TESTCONTAINERS_RYUK_DISABLED"] = "true"

import pytest
from testcontainers.community.postgres import PostgresContainer
from sqlalchemy import text

from utils.connections.database import connect_to_db, DatabaseCall


@pytest.fixture(scope="module")
def postgres_engine():
    with PostgresContainer("postgres:16") as postgres:
        url = postgres.get_connection_url()
        engine = connect_to_db(url)
        yield engine
        engine.dispose()

def test_connection__postgres(postgres_engine):
    engine = connect_to_db(postgres_engine.url)
    assert engine is not None
    engine.dispose()

def test_call__postgres(postgres_engine):
    with postgres_engine.connect() as connection:

        call = DatabaseCall(connection)
        assert call is not None

def test_yield_records__postgres(postgres_engine):
    with postgres_engine.connect() as connection:
        call = DatabaseCall(connection)

        connection.execute(text("""
            CREATE TABLE test (
                id INTEGER PRIMARY KEY,
                event_name TEXT
            )
        """))

        connection.execute(text("""
            INSERT INTO test (id, event_name)
            VALUES
                (1, 'created'),
                (2, 'updated'),
                (3, 'deleted'),
                (4, 'completed')
        """))

        result = call.yield_records(
            schema="public",
            object="test",
        )

        records = list(result)

        assert len(records) == 4
        assert records[0]["id"] == 1
        assert records[1]["id"] == 2
        assert records[2]["id"] == 3
        assert records[3]["id"] == 4

        for record in records:
            assert "_dlt_processed_utc" in record

def test_yield_records_where_clause__postgres(postgres_engine):
    with postgres_engine.connect() as connection:
        call = DatabaseCall(connection)

        connection.execute(text("""
            CREATE TABLE test (
                id INTEGER PRIMARY KEY,
                event_name TEXT
            )
        """))

        connection.execute(text("""
            INSERT INTO test (id, event_name)
            VALUES
                (1, 'created'),
                (2, 'updated'),
                (3, 'deleted'),
                (4, 'completed')
        """))

        result = call.yield_records(
            schema="public",
            object="test",
            where_clause="id = 4",
        )

        records = list(result)

        assert len(records) == 1
        assert records[0]["id"] == 4

        for record in records:
            assert "_dlt_processed_utc" in record

def test_yield_records_incremental__postgres(postgres_engine):
    with postgres_engine.connect() as connection:
        call = DatabaseCall(connection)

        connection.execute(text("""
            CREATE TABLE test (
                id INTEGER PRIMARY KEY,
                event_name TEXT
            )
        """))

        connection.execute(text("""
            INSERT INTO test (id, event_name)
            VALUES
                (1, 'created'),
                (2, 'updated'),
                (3, 'deleted'),
                (4, 'completed')
        """))

        result = call.yield_records(
            schema="public",
            object="test",
            where_clause="true",
            incremental={
                "attribute": "id",
                "value": 2,
            },
        )

        records = list(result)

        assert len(records) == 2
        assert records[0]["id"] == 3
        assert records[1]["id"] == 4

        for record in records:
            assert "_dlt_processed_utc" in record