import dlt
from dlt.sources import incremental

import re

class APIResource:
    def __init__(
        self,
        object: str,
        call: object,
        write_disposition: str = "append",
        incremental_attribute: str | None = None,
        incremental_initial_value: int = 0,
        api_source: str | None = None,
        **kwargs,
    ):

        """
            incremental_details expects a dictionary with keys:
                - attribute: The attribute used for incremental loading
                - initial_value: The initial value for the incremental attribute
        
            i.e.
            incremental_details = {
                "attribute": "created",
                "initial_value": 0,
            }
        """

        self.call = call
        self.object = object
        self.api_source = api_source
        self.write_disposition = write_disposition

        if api_source == "stripe" and incremental_attribute:
            cleaned_incremental_attribute = re.sub(r"\[.*?\]", "", incremental_attribute)
        else:
            cleaned_incremental_attribute = incremental_attribute

        self.incremental_attribute = incremental_attribute
        self.incremental_obj = (
            incremental(cleaned_incremental_attribute, initial_value=incremental_initial_value)
            if incremental_attribute else None
        )

        # Expected kwargs
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
            # primary_key=None -> to record history of slow changing fields
            yield from self.yield_query_results(incremental_obj=incremental_obj)
        return my_resource()
