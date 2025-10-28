import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from core.pipeline import LoadPipelineS3
from utils.connections.stripe import StripeCall

from utils.helpers import pretty_all_jsons

from env import (
    STRIPE_API_SECRET,
    STRIPE_OBJECTS
)


class FullPipeline:
    def __init__(
        self,
        objects = STRIPE_OBJECTS.split(','),
        test=False
    ):

        self.objects = objects
        self.test = test

        self.pipeline = None
        self.call = None
        self.resources = None

    def create_call(
        self,
        api_secret=STRIPE_API_SECRET,
    ):
        self.call = StripeCall(
            api_secret=api_secret,
            test=self.test,
        )
        return self.call

    def create_pipeline(self):
        self.pipeline = LoadPipelineS3(
            pipeline_name='stripe_to_s3',
            dataset='stripe',
            test=self.test
        )
        return self.pipeline

    def build_resources(self):
        self.resources = self.pipeline.build_api_resources(
            api_call=self.call,
            sources=self.objects,
            incremental_attribute='created[gte]',
            write_disposition='append',
            table_name_suffix='__incr_created',
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