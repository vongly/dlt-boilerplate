import dlt
import duckdb

import os
import gzip
import re
import json

from config import (
    PIPELINES_DIR
)

class Pipeline:
    def __init__(
        self,
        resources: object | list[object],
        pipeline_name: str,
        destination: str,
        dataset: str,
        pipelines_dir: str = PIPELINES_DIR,
        file_format: str = "parquet",
        test: bool = False,
        **destination_env,
    ):

        self.pipeline_name = "_test_" + pipeline_name if test else pipeline_name
        self.resources = resources if isinstance(resources, list) else [resources]
        self.file_format = file_format

        self.pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=destination,
            dataset_name=dataset,
            pipelines_dir=pipelines_dir,
        )

        for name, value in (destination_env or {}).items():
            os.environ[name] = value

    def run_pipeline(self):
#        print_pipeline_details(self.pipeline)
        jobs = []

        load_info = self.pipeline.run(self.resources, loader_file_format=self.file_format)
        load_packages = load_info.load_packages

        for package in load_packages:
            completed_jobs = package.jobs.get("completed_jobs", [])
            failed_jobs = package.jobs.get("failed_jobs", [])

            for job in completed_jobs + failed_jobs:
                job_details = {}
                job_details["table"] = getattr(job.job_file_info, "table_name", None)
                job_details["state"] = getattr(job, "state", None)
                job_details["file_path"] = getattr(job, "file_path", None)
                job_details["MB"] = round(getattr(job, "file_size", None)/1000000, 2) if getattr(job, "file_size", None) else None
                job_details["seconds_elapsed"] = round(getattr(job, "elapsed", None), 2) if getattr(job, "elapsed", None) else None
                job_details["file_id"] = getattr(job, "file_id", None)

                
                if job.file_path.endswith(".parquet"):
                    job_details["records"] = duckdb.sql(f"""select count(*) from read_parquet("{job.file_path}")""").fetchall()[0][0]
                elif job.file_path.endswith(".jsonl.gz"):
                    job_details["records"] = duckdb.sql(f"""select count(*) from read_json("{job.file_path}")""").fetchall()[0][0]
                elif job.file_path.endswith(".insert_values.gz"):
                    with gzip.open(job.file_path, "rt", encoding="utf-8") as f:
                        text = f.read()
                    job_details["records"] = len(re.findall(r"\([^\)]*\)", text))

                jobs.append(job_details)

        self.jobs = sorted(jobs, key=lambda x: x["table"])
        self.jobs_json = json.dumps(self.jobs, indent=2)

    def print_output(self):
        print(self.jobs_json)
