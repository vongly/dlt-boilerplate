import dlt
from dlt.sources import incremental


class DatabaseResource:
    def __init__(
        self,
        object_name,
        db_call,
        incremental_attribute=None,
        write_disposition='append',
        **kwargs,
    ):

        self.db_call = db_call
        self.object_name = object_name

        self.incremental_attribute = incremental_attribute
        self.incremental_obj = incremental(incremental_attribute, initial_value=None) if incremental_attribute is not None else None
        self.write_disposition = write_disposition


        # Expected kwargs
        self.schema = kwargs.get('schema', None)
        self.table_name_suffix = kwargs.get('table_name_suffix', '')
        
    def yield_query_results(self, schema=None, incremental_obj=None):
        # Incremental Filter -> requires incremental attribute
        if self.incremental_attribute:
            if incremental_obj:
                if incremental_obj.last_value:
                    incremental_value = incremental_obj.last_value
                else:
                    incremental_value = None
            else:
                incremental_value = None
        else:
            incremental_value = None

        args = {
            'object_name': self.object_name,
            'incremental': {
                'attribute': self.incremental_attribute,
                'value': incremental_value,
            }
        }

        if schema:
            args['schema'] = schema

        yield from self.db_call.yield_records(**args)

    def create_resource(self):
        table_name = self.object_name + self.table_name_suffix

        @dlt.resource(
            name=table_name,
            table_name=table_name,
            write_disposition=self.write_disposition,
            primary_key=None,
        )
        def my_resource(incremental_obj=self.incremental_obj):
            schema = self.schema
            yield from self.yield_query_results(incremental_obj=incremental_obj, schema=schema)
        return my_resource()