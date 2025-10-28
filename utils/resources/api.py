import dlt
from dlt.sources import incremental

import re

class APIResource:
    def __init__(
            self,
            object_name,
            api_call,
            write_disposition='append',
            incremental_attribute=None,
            **kwargs,
        ):

        self.api_call = api_call
        self.object_name = object_name
        self.write_disposition = write_disposition

        # Stripe
        if '[' in incremental_attribute: # greater/less than operators -> [gte]/[gt]/[lte]/[lt]
            cleaned_incremental_attribute = re.sub(r"\[.*?\]", "", incremental_attribute)
        # Shopify
        elif incremental_attribute[-4:] in ['_min', '_max']: # _min returns all records greater/equal to | _max -> opposite
            cleaned_incremental_attribute = incremental_attribute[:-4]
        else:
            cleaned_incremental_attribute = incremental_attribute

        self.incremental_attribute = incremental_attribute
        self.incremental_obj = incremental(cleaned_incremental_attribute, None) if incremental_attribute else None


        # Expected kwargs
        self.table_name_suffix = kwargs.get('table_name_suffix', '')

    def yield_query_results(self, incremental_obj=None):
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

        yield from self.api_call.yield_records(**args)

    def create_resource(self):
        table_name = self.object_name + self.table_name_suffix

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

