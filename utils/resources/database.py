from datetime import datetime, timezone

import dlt
from dlt.sources import incremental


class DatabaseResource:
    def __init__(
        self,
        object: str,
        call: object,
        write_disposition: str = "append",
        incremental_attribute: str | None = None,
        incremental_initial_value: int | str | datetime | None = datetime(1970, 1, 1, tzinfo=timezone.utc),
        **kwargs,
    ):

        self.call = call
        self.object = object

        self.incremental_attribute = incremental_attribute
        self.incremental_obj = (
            incremental(incremental_attribute, initial_value=incremental_initial_value)
            if incremental_attribute
            is not None else None
        )
        self.write_disposition = write_disposition

        # Expected kwargs
        self.schema = kwargs.get("schema", None)
        self.table_name_prefix = kwargs.get("table_name_prefix", "")
        self.table_name_suffix = kwargs.get("table_name_suffix", "")
        
    def yield_query_results(self, incremental_obj: incremental | None = None):
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
            "object": self.object,
            "incremental": {
                "attribute": self.incremental_attribute,
                "value": incremental_value,
            }
        }

        if self.schema:
            args["schema"] = self.schema

        yield from self.call.yield_records(**args)

    def create_resource(self):
        table_name = f"{self.table_name_prefix}{self.object}{self.table_name_suffix}"

        @dlt.resource(
            name=table_name,
            table_name=table_name,
            write_disposition=self.write_disposition,
            primary_key=None,
        )
        def my_resource(incremental_obj=self.incremental_obj):
            schema = self.schema
            yield from self.yield_query_results(incremental_obj=incremental_obj)
        return my_resource()