import dlt
import duckdb

import sys, os
from pathlib import Path
import gzip
import re
import json

from utils.helpers import print_pipeline_details
from utils.resources.api import APIResource
from utils.resources.database import DatabaseResource

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from env import (
    PIPELINES_DIR,
    S3_ACCESS_KEY,
    S3_SECRET_ACCESS_KEY,
    S3_REGION,
    S3_ENDPOINT_URL,
)


class CreatePipeline:
    def __init__(
        self,
        resources: object,
        pipeline_name: str,
        destination: str,
        dataset: str,
        pipelines_dir=PIPELINES_DIR,
        **kwargs,
    ):

        self.pipeline_object = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=destination,
            dataset_name=dataset,
            pipelines_dir=pipelines_dir,
        )
        
        self.resources = resources if isinstance(resources, list) else [resources]

    def run_pipeline(self):
        print_pipeline_details(self.pipeline_object)
        jobs = []

        load_info = self.pipeline_object.run(self.resources, loader_file_format='parquet')
        load_packages = load_info.load_packages

        for package in load_packages:
            completed_jobs = package.jobs.get('completed_jobs', [])
            failed_jobs = package.jobs.get('failed_jobs', [])

            for job in completed_jobs + failed_jobs:
                job_details = {}
                job_details['table'] = getattr(job.job_file_info, 'table_name', None)
                job_details['state'] = getattr(job, 'state', None)
                job_details['file_path'] = getattr(job, 'file_path', None)
                job_details['MB'] = round(getattr(job, 'file_size', None)/1000000, 2) if getattr(job, 'file_size', None) else None
                job_details['seconds_elapsed'] = round(getattr(job, 'elapsed', None), 2) if getattr(job, 'elapsed', None) else None
                job_details['file_id'] = getattr(job, 'file_id', None)

                
                if job.file_path.endswith('.parquet'):
                    job_details['records'] = duckdb.sql(f'''select count(*) from read_parquet('{job.file_path}')''').fetchall()[0][0]
                elif job.file_path.endswith('.jsonl.gz'):
                    job_details['records'] = duckdb.sql(f'''select count(*) from read_json('{job.file_path}')''').fetchall()[0][0]
                elif job.file_path.endswith('.insert_values.gz'):
                    with gzip.open(job.file_path, "rt", encoding="utf-8") as f:
                        text = f.read()
                    job_details['records'] = len(re.findall(r'\([^\)]*\)', text))

                jobs.append(job_details)

        self.jobs = sorted(jobs, key=lambda x: x['table'])
        self.jobs_json = json.dumps(self.jobs, indent=2)


class LoadPipelineS3:
    def __init__(
        self,
        pipeline_name,
        dataset,
        destination='filesystem',
        s3_access_key=S3_ACCESS_KEY,
        s3_secret_access_key=S3_SECRET_ACCESS_KEY,
        s3_region=S3_REGION,
        s3_endpoint_url=S3_ENDPOINT_URL,
        test=False,
    ):
        
        if test:
            self.pipeline_name = '_test_' + pipeline_name 
        else:
            self.pipeline_name = pipeline_name

        self.dataset = dataset
        self.destination = destination
        self.test = test
        self.s3_bucket = f's3://pipelines/{self.pipeline_name}'

        os.environ[f'{self.pipeline_name.upper()}__DESTINATION__FILESYSTEM__BUCKET_URL'] = self.s3_bucket
        os.environ[f'{self.pipeline_name.upper()}__DESTINATION__CREDENTIALS__ENDPOINT_URL'] = s3_endpoint_url
        os.environ[f'{self.pipeline_name.upper()}__DESTINATION__CREDENTIALS__AWS_ACCESS_KEY_ID'] = s3_access_key
        os.environ[f'{self.pipeline_name.upper()}__DESTINATION__CREDENTIALS__AWS_SECRET_ACCESS_KEY'] = s3_secret_access_key
        os.environ[f'{self.pipeline_name.upper()}__DESTINATION__CREDENTIALS__AWS_DEFAULT_REGION'] = s3_region

    def build_api_resources(
        self,
        api_call,
        sources,
        incremental_attribute,
        write_disposition,
        **kwargs,
    ):
        table_name_suffix = kwargs.get('table_name_suffix', '')

        resources = [
            APIResource(
                object_name=source,
                api_call=api_call,
                incremental_attribute=incremental_attribute,
                write_disposition=write_disposition,
                table_name_suffix=table_name_suffix, # Optional
            ).create_resource()

                for source in sources
        ]

        return resources

    def build_db_resources(
        self,
        db_call,
        sources,
        incremental_attribute,
        write_disposition,
        **kwargs,
    ):
        schema = kwargs.get('schema', None)
        table_name_suffix = kwargs.get('table_name_suffix', '')
        
        resources = [
            DatabaseResource(
                object_name=source,
                db_call=db_call,
                incremental_attribute=incremental_attribute,
                write_disposition=write_disposition,
                schema=schema,                       # schema not required for embedded db's
                table_name_suffix=table_name_suffix, # Optional
            ).create_resource()

                for source in sources
        ]
        
        return resources

    def build_pipeline(self, resources):
        self.pipeline = CreatePipeline(
            resources=resources,
            pipeline_name=self.pipeline_name,
            dataset=self.dataset,
            destination=self.destination,
        )

        return self.pipeline

    def run_pipeline(self):
        self.pipeline.run_pipeline()

    def print_output(self):
        print('Expected output:', self.s3_bucket, '\n')
        print(self.pipeline.jobs_json)
