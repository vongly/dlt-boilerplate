from collections.abc import Generator, Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import DateTime, MetaData, Table, bindparam, literal, select, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine import URL, create_engine

"""

    Possible Drivers

    DB          drivername          package

    PostgreSQL  postgresql+psycopg	psycopg
    PostgreSQL  postgresql+psycopg2	psycopg2-binary
    PostgreSQL  postgresql+asyncpg	asyncpg
    MySQL	    mysql+pymysql	    pymysql
    MySQL	    mysql+mysqldb	    mysqlclient
    SQLite	    sqlite	            Built-in sqlite3
    SQL Server	mssql+pyodbc	    pyodbc
    Oracle	    oracle+oracledb	    oracledb
    
    *NOT SUPPORTED
    DuckDB	    duckdb              duckdb

"""

def connect_to_db(url: str | URL, **engine_options) -> Engine:
    """
        **engine_options:

        echo: bool
        pool_pre_ping: bool
        pool_recycle: int

    """

    return create_engine(url, **engine_options)


class DatabaseCall:
    def __init__(
        self,
        connection: Engine | Connection,
        test: bool = False,
        test_limit: int = 1,
    ) -> None:

        self.connection = connection
        self.test = test
        self.test_limit = test_limit

        if self.test and (not self.test_limit or self.test_limit < 1):
            self.test_limit = 1

    def yield_records(
        self,
        schema: str | None,
        object: str,
        where_clause: str | None = None,
        incremental: Mapping[str, Any] | None = None,
        processed_timestamp: datetime = datetime.now(timezone.utc),
    ) -> Iterator[dict[str, Any]]:

        with self._connection_scope() as connection:
            table = Table(
                object,
                MetaData(),
                schema=schema or None,
                autoload_with=connection,
            )

            statement = select(
                table,
                literal(processed_timestamp, type_=DateTime(timezone=True)).label("_dlt_processed_utc"),
            )
            
            conditions = []

            if where_clause:
                condition = where_clause.strip()
                if condition[:6].lower() == "where ":
                    condition = condition[6:].strip()
                conditions.append(text(condition))

            if incremental:
                attribute = incremental["attribute"]
                conditions.append(
                    table.c[attribute] > bindparam("incremental_value", incremental["value"])
                )

            if conditions:
                statement = statement.where(*conditions)

            if self.test:
                statement = statement.limit(self.test_limit)

            result = connection.execute(statement)
            for row in result.mappings():
                yield dict(row)

    @contextmanager
    def _connection_scope(self) -> Generator[Connection, None, None]:
        if isinstance(self.connection, Engine):
            with self.connection.connect() as connection:
                yield connection
        else:
            yield self.connection
