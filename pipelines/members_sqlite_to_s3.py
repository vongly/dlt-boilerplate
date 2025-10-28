import sys, os
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from core.pipeline import LoadPipelineS3
from utils.connections.sqlite import AccessSqliteDB, SQLiteCall

from utils.helpers import pretty_all_jsons

from env import (
    MEMBERS_SQLITE_LOCATION_IP,
    MEMBERS_SQLITE_LOCATION_USER,
    MEMBERS_SQLITE_LOCATION_PASS,
    MEMBERS_SQLITE_LOCATION_FILEPATH,
    MEMBERS_SQLITE_LOCAL_DESTINATION,
    MEMBERS_SQLITE_OBJECTS,
)


class FullPipeline:
    def __init__(
        self,
        objects = MEMBERS_SQLITE_OBJECTS.split(','),
        test=False
    ):

        self.objects = objects
        self.test = test

        self.pipeline = None
        self.call = None
        self.resources = None

    def create_call(
        self,
        sqlite_remote_server_ip=MEMBERS_SQLITE_LOCATION_IP,
        sqlite_remote_server_user=MEMBERS_SQLITE_LOCATION_USER,
        sqlite_remote_server_pass=MEMBERS_SQLITE_LOCATION_PASS,
        sqlite_remote_server_file_path=MEMBERS_SQLITE_LOCATION_FILEPATH,
        sqlite_local_destination=MEMBERS_SQLITE_LOCAL_DESTINATION,
        sqlite_filename = 'bjj_copy.sqlite',
    ):
        AccessSqliteDB(
            sqlite_remote_server_ip=sqlite_remote_server_ip,
            sqlite_remote_server_user=sqlite_remote_server_user,
            sqlite_remote_server_pass=sqlite_remote_server_pass,
            sqlite_remote_server_file_path=sqlite_remote_server_file_path,
            sqlite_local_destination=sqlite_local_destination,
        ).copy_sqlite_db(rename=sqlite_filename)

        self.call = SQLiteCall(
            db_path=os.path.join(sqlite_local_destination, sqlite_filename),
            test=self.test,
        )
        return self.call

    def create_pipeline(self):
        self.pipeline = LoadPipelineS3(
            pipeline_name='members_sqlite_to_s3',
            dataset='members_sqlite',
            test=self.test
        )
        return self.pipeline

    def build_resources(self):
        self.resources = self.pipeline.build_api_resources(
            api_call=self.call,
            sources=self.objects,
            incremental_attribute='updated_at',
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