import gzip
import json
import os
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

# config.py parses POSTGRES_PORT during import, so provide a harmless default
# for test collection when the developer's environment doesn't define it.
os.environ.setdefault("POSTGRES_PORT", "5432")

import core.pipeline as pipeline_module
from core.pipeline import Pipeline


@pytest.fixture
def make_pipeline(monkeypatch):
    pipeline_client = Mock()
    pipeline_calls = []

    def fake_dlt_pipeline(**kwargs):
        pipeline_calls.append(kwargs)
        return pipeline_client

    monkeypatch.setattr(pipeline_module.dlt, "pipeline", fake_dlt_pipeline)

    def _make_pipeline(**kwargs):
        options = {
            "resources": "customers",
            "pipeline_name": "stripe_to_s3",
            "destination": "filesystem",
            "dataset": "stripe",
            "pipelines_dir": "/tmp/test-pipelines",
        }
        options.update(kwargs)
        return Pipeline(**options), pipeline_client, pipeline_calls

    return _make_pipeline


def make_job(table, file_path, **attributes):
    job = {
        "job_file_info": SimpleNamespace(table_name=table),
        "state": "completed",
        "file_path": str(file_path),
        "file_size": 2_000_000,
        "elapsed": 1.234,
        "file_id": f"{table}-file",
    }
    job.update(attributes)
    return SimpleNamespace(**job)


def test_initialization_creates_dlt_pipeline_and_normalizes_resources(make_pipeline):
    resource = object()

    pipeline, _, pipeline_calls = make_pipeline(resources=resource)

    assert pipeline.resources == [resource]
    assert pipeline.pipeline_name == "stripe_to_s3"
    assert pipeline.file_format == "parquet"
    assert pipeline_calls == [
        {
            "pipeline_name": "stripe_to_s3",
            "destination": "filesystem",
            "dataset_name": "stripe",
            "pipelines_dir": "/tmp/test-pipelines",
        }
    ]


def test_initialization_keeps_resource_list_and_configures_destination_env(
    make_pipeline,
    monkeypatch,
):
    monkeypatch.delenv("TEST_PIPELINE_DESTINATION", raising=False)
    resources = [object(), object()]

    pipeline, _, _ = make_pipeline(
        resources=resources,
        file_format="jsonl",
        TEST_PIPELINE_DESTINATION="test-bucket",
    )

    assert pipeline.resources == resources
    assert pipeline.file_format == "jsonl"
    assert pipeline.pipeline_name == "stripe_to_s3"
    assert os.environ["TEST_PIPELINE_DESTINATION"] == "test-bucket"


def test_run_pipeline_collects_counts_and_sorts_jobs(
    make_pipeline,
    monkeypatch,
    tmp_path,
):
    parquet_path = tmp_path / "zebra.parquet"
    jsonl_path = tmp_path / "apple.jsonl.gz"
    values_path = tmp_path / "banana.insert_values.gz"
    values_path.write_bytes(gzip.compress(b"(1, 'a'),(2, 'b')"))

    parquet_job = make_job("zebra", parquet_path)
    jsonl_job = make_job("apple", jsonl_path, state="completed")
    values_job = make_job("banana", values_path, state="failed")

    pipeline, pipeline_client, _ = make_pipeline(
        resources=["resource-a", "resource-b"],
        file_format="jsonl",
    )
    pipeline_client.run.return_value.load_packages = [
        SimpleNamespace(
            jobs={
                "completed_jobs": [parquet_job, jsonl_job],
                "failed_jobs": [values_job],
            }
        )
    ]

    duckdb_result = Mock()
    duckdb_result.fetchall.return_value = [[3]]
    duckdb_sql = Mock(return_value=duckdb_result)
    monkeypatch.setattr(pipeline_module.duckdb, "sql", duckdb_sql)

    pipeline.run_pipeline()

    pipeline_client.run.assert_called_once_with(
        ["resource-a", "resource-b"],
        loader_file_format="jsonl",
    )
    assert [job["table"] for job in pipeline.jobs] == [
        "apple",
        "banana",
        "zebra",
    ]
    assert {job["table"]: job["records"] for job in pipeline.jobs} == {
        "apple": 3,
        "banana": 2,
        "zebra": 3,
    }

    apple_job = pipeline.jobs[0]
    assert apple_job["state"] == "completed"
    assert apple_job["MB"] == 2.0
    assert apple_job["seconds_elapsed"] == 1.23
    assert apple_job["file_id"] == "apple-file"
    assert json.loads(pipeline.jobs_json) == pipeline.jobs
    assert duckdb_sql.call_count == 2
    assert "read_parquet" in duckdb_sql.call_args_list[0].args[0]
    assert "read_json" in duckdb_sql.call_args_list[1].args[0]


def test_run_pipeline_handles_packages_without_jobs(make_pipeline):
    pipeline, pipeline_client, _ = make_pipeline()
    pipeline_client.run.return_value.load_packages = [
        SimpleNamespace(jobs={})
    ]

    pipeline.run_pipeline()

    assert pipeline.jobs == []
    assert pipeline.jobs_json == "[]"


def test_print_output_prints_json(capsys, make_pipeline):
    pipeline, _, _ = make_pipeline()
    pipeline.jobs_json = '[{"table": "customers"}]'

    pipeline.print_output()

    assert capsys.readouterr().out == '[{"table": "customers"}]\n'
