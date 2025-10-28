import sys
from pathlib import Path
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))


def connect_to_postgres(
    dbname,
    user,
    password,
    host,
    port,
):
    connection = psycopg.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
        row_factory=dict_row,
    )

    return connection

class PostgresCall():
    def __init__(
        self,
        connection: callable,
        test=False,
        test_limit=1,
    ):
    
        self.connection = connection
        self.test = test
        self.test_limit = test_limit

    def yield_records(
        self,
        table_name,
        where_clause= None,
        incremental:dict=None,
        processed_timestamp=datetime.now(timezone.utc),
        **kwargs,
    ):
        
        # Handles schema if applicable
        schema = kwargs.get('schema', None)
        table_name = f'{schema}.{table_name}' if schema else table_name

        if where_clause and not incremental:
            pass # -> where_clause = where_clause
        elif not where_clause and incremental:
            where_clause = f''' WHERE {incremental['attribute']} > '{incremental['value']}' '''
        elif where_clause and incremental:
            where_clause = f''' {where_clause} AND {incremental['attribute']} > '{incremental['value']}' '''
        else:
            where_clause = ''

        if self.test:
            if not self.test_limit or self.test_limit < 1:
                self.test_limit = 1
        
        limit_clause = f' LIMIT {self.test_limit}' if self.test else ''

        query = f'''
                select
                    *,
                    cast('{processed_timestamp}' as timestamp) as _dlt_processed_utc
                from
                    {table_name} 
            ''' + where_clause + limit_clause + ';'


        with self.connection.cursor() as cursor:
            cursor.execute(query)

            rows = cursor.fetchall()
            for row in rows:
                yield row


if __name__ == '__main__':
    pass