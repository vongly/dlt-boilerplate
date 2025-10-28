import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from core.pipeline import LoadPipelineS3
from utils.connections.shopify import ShopifyCall

from utils.helpers import pretty_all_jsons

from env import (
    SHOPIFY_ACCESS_TOKEN,
    SHOPIFY_STORE_NAME,
    SHOPIFY_API_VERSION,
    SHOPIFY_OBJECTS,
)


class FullPipeline:
    def __init__(
        self,
        objects=SHOPIFY_OBJECTS.split(','),
        test=False
    ):

        self.objects = objects
        self.test = test

        # Built with class functions
        self.pipeline = None
        self.call = None
        self.resources = None

    def create_call(
        self,
        access_token=SHOPIFY_ACCESS_TOKEN,
        store_name=SHOPIFY_STORE_NAME,
        api_version=SHOPIFY_API_VERSION,
    ):
        self.call = ShopifyCall(
            access_token=access_token,
            store_name=store_name,
            api_version=api_version,
            test=self.test,
        )
        return self.call

    def create_pipeline(self):
        self.pipeline = LoadPipelineS3(
            pipeline_name='shopify_to_s3',
            dataset='shopify',
            test=self.test
        )
        return self.pipeline

    def build_resources(self):
        self.resources = self.pipeline.build_api_resources(
            api_call=self.call,
            sources=self.objects,
            incremental_attribute='updated_at_min',
            write_disposition='append',
            table_name_suffix='__incr_updated_at',
        )
        return self.resources

    def load_resources(self):
        pipeline_loaded = self.pipeline.build_pipeline(resources=self.resources)
        return pipeline_loaded

    def run_pipeline(self):
        self.pipeline.run_pipeline()
        return True
    
    def print_output(self):
        self.pipeline.print_output()
        pretty_all_jsons()

    def run_all(self):
        self.create_call()
        self.create_pipeline()
        self.build_resources()
        self.load_resources()
        self.run_pipeline()
        self.print_output()

if __name__ == '__main__':
    pipeline = FullPipeline()
    pipeline.run_all()
