import os, sys
from pathlib import Path
import json
import re

import psycopg
import duckdb

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from env import (
    PROJECT_DIRECTORY,
    EXTRACT_DIR,
)

def print_pipeline_details(pipeline):
    pipeline_name = pipeline.pipeline_name
    destination = pipeline.destination.__class__.__name__.lower()
    dataset = pipeline.dataset_name
    working_dir = pipeline.pipelines_dir

    print('\n', ' RUNNING NEW PIPELINE -')
    print('  Name:', pipeline_name)
    if destination == 'filesystem':
        print('  Destination:', EXTRACT_DIR)
    else:
        print('  Destination:', destination)
    print('  Dataset:', dataset)
    print('  Working dir:', working_dir, '\n')

def pretty_print_json_file(input_path, output_path):
    try:
        with open(input_path, 'r') as infile:
            data = json.load(infile)

        with open(output_path, 'w') as outfile:
            json.dump(data, outfile, indent=2)

    except Exception as e:
        pass

def pretty_all_jsons(base_dir=PROJECT_DIRECTORY):
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.json'):
                input_path = os.path.join(root, file)
                output_path = os.path.join(root, file)
                pretty_print_json_file(input_path, output_path)


def get_all_data_files(file_extension, directory=EXTRACT_DIR):    
    pipelines = os.listdir(directory)

    datasets = []
    for pipeline in pipelines:
        dataset = os.listdir(f'{directory}/{pipeline}')[0]
        datasets.append(f'{directory}/{pipeline}/{dataset}')

    dlt_folders = [
        '_dlt_loads',
        '_dlt_pipeline_state',
        '_dlt_version',
        'init',
    ]

    tables = []
    for dataset in datasets:
        dataset_tables = os.listdir(dataset)
        for dataset_table in dataset_tables:
            if dataset_table not in dlt_folders:
                tables.append(f'{dataset}/{dataset_table}')

    all_data_files = []
    for table in tables:
        data_files = os.listdir(table)
        for data_file in data_files:
            if data_file.endswith(file_extension):
                all_data_files.append(f'{table}/{data_file}')

    return all_data_files

def query_data_files(pipeline, dataset, query):
    query_cleaned = re.sub(r'\s+', ' ', query.replace('\n',' ').lower())

    from_pos = query_cleaned.find(' from ') + 1
    table_start_pos = query_cleaned.find(' ', from_pos) + 1
    table_end_pos = query_cleaned.find(' ', table_start_pos)

    table_name = query_cleaned[table_start_pos:table_end_pos]

    table_dir_string = f'{EXTRACT_DIR}/{pipeline}/{dataset}{table_name}/*.parquet'

    query_w_table_dir_path = query_cleaned.replace(table_name, f'read_parquet({table_dir_string})')

    results = duckdb.sql(query_w_table_dir_path)

    return results

def query_s3_data_files(pipeline, dataset, query, access_key, private_access_key, region, endpoint, bucket):
    credentials = f'''
        LOAD httpfs;
        SET s3_access_key_id = '{access_key}';
        SET s3_secret_access_key = '{private_access_key}';
        SET s3_region = '{region}';
        SET s3_endpoint = '{endpoint}';
        SET s3_url_style = 'path';

    '''

    query_cleaned = re.sub(r'\s+', ' ', query.replace('\n',' ').lower())

    from_pos = query_cleaned.find(' from ') + 1
    table_start_pos = query_cleaned.find(' ', from_pos) + 1
    table_end_pos = query_cleaned.find(' ', table_start_pos)

    table_name = query_cleaned[table_start_pos:table_end_pos]

    table_dir_path = f''' '{bucket}/{pipeline}/{dataset}/{table_name}/*.parquet' '''.strip()

    query_w_table_dir_path = credentials + query_cleaned.replace(table_name, f'read_parquet({table_dir_path})')

    results = duckdb.sql(query_w_table_dir_path)

    return results

def convert_data_files_to_parquet():
    data_files = get_all_data_files('.jsonl.gz')

    output = []

    for data_file in data_files:
        parquet_file = data_file.replace('.jsonl.gz','.parquet')
        duckdb.sql(f'''
            COPY (SELECT * FROM read_json_auto('{data_file}'))
            TO '{parquet_file}' (FORMAT PARQUET);
        '''
        )

        os.remove(data_file)
        
        output.append({
            'jsonl.gz': data_file,
            'parquet': parquet_file
        })

    return output

