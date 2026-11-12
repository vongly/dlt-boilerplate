import os
import json

from config import (
    PROJECT_DIRECTORY,
    PIPELINES_DIR,
)

def print_pipeline_details(pipeline):
    pipeline_name = pipeline.pipeline_name
    destination = pipeline.destination.__class__.__name__.lower()
    dataset = pipeline.dataset_name
    pipeline_dir = pipeline.pipelines_dir

    print("\n", " RUNNING NEW PIPELINE -")
    print("  Name:", pipeline_name)
    if destination == "filesystem":
        print("  Destination:", PIPELINES_DIR)
    else:
        print("  Destination:", destination)
    print("  Dataset:", dataset)
    print("  Working dir:", pipeline_dir, "\n")

def pretty_all_jsons(base_dir=PROJECT_DIRECTORY):
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".json"):
                input_path = os.path.join(root, file)
                output_path = os.path.join(root, file)

                try:
                    with open(input_path, "r") as infile:
                        data = json.load(infile)

                    with open(output_path, "w") as outfile:
                        json.dump(data, outfile, indent=2)

                except Exception as e:
                    pass

