
from core.pipeline import Pipeline
from utils.resources.api import APIResource
from utils.connections.stripe import StripeCall

from config import (
    S3_PIPELINE_STAGING_BUCKET_URL,
    S3_PIPELINE_STAGING_ACCESS_KEY,
    S3_PIPELINE_STAGING_SECRET_ACCESS_KEY,
    S3_PIPELINE_STAGING_REGION,
    S3_PIPELINE_STAGING_ENDPOINT_URL,
    STRIPE_KEY,
)


def stripe_to_s3():
    objects = ["customers", "invoices"]

    call = StripeCall(api_secret=STRIPE_KEY)

    resources = [
        APIResource(
            object=object,
            call=call,
            incremental_attribute="created",
            incremental_initial_value=0,
            write_disposition="append",
            api_source="stripe",
            table_name_suffix="__incr_created",
        ).create_resource()

        for object in objects
    ]

    pipeline_name="stripe_to_s3"

    destination_env = {
        f"{pipeline_name.upper()}__DESTINATION__FILESYSTEM__BUCKET_URL": S3_PIPELINE_STAGING_BUCKET_URL,
        f"{pipeline_name.upper()}__DESTINATION__CREDENTIALS__ENDPOINT_URL": S3_PIPELINE_STAGING_ENDPOINT_URL,
        f"{pipeline_name.upper()}__DESTINATION__CREDENTIALS__AWS_ACCESS_KEY_ID": S3_PIPELINE_STAGING_ACCESS_KEY,
        f"{pipeline_name.upper()}__DESTINATION__CREDENTIALS__AWS_SECRET_ACCESS_KEY": S3_PIPELINE_STAGING_SECRET_ACCESS_KEY,
        f"{pipeline_name.upper()}__DESTINATION__CREDENTIALS__AWS_DEFAULT_REGION": S3_PIPELINE_STAGING_REGION,
    }

    pipeline = Pipeline(
        resources=resources,
        pipeline_name=pipeline_name,
        destination="filesystem",
        dataset="stripe_to_s3",
        **destination_env,
    )

    pipeline.run_pipeline()
    pipeline.print_output()
