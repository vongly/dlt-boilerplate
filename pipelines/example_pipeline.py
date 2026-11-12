from datetime import datetime, timezone
from sqlalchemy import URL


from core.pipeline import Pipeline
from utils.resources.database import DatabaseResource
from utils.connections.database import connect_to_db, DatabaseCall
from utils.connections.ssh_tunnel import ssh_tunnel

from config import (
    S3_PIPELINE_STAGING_BUCKET_URL,
    S3_PIPELINE_STAGING_ACCESS_KEY,
    S3_PIPELINE_STAGING_SECRET_ACCESS_KEY,
    S3_PIPELINE_STAGING_REGION,
    S3_PIPELINE_STAGING_ENDPOINT_URL,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_DATABASE,
    POSTGRES_SSH_TUNNEL_TARGET,
    POSTGRES_SSH_TUNNEL_KEY,
)

"""

This is an example pipeline that loads data from PostgreSQL to S3.

"""

def example_postgres_to_s3():
    # Defines the tables to load
    objects = ["students", "subscriptions"]

    local_port = 4000

    # Establish an SSH tunnel to the PostgreSQL database
    with ssh_tunnel(
        ssh_target=POSTGRES_SSH_TUNNEL_TARGET,
        remote_host=POSTGRES_HOST,
        remote_port=POSTGRES_PORT,
        local_port=local_port,
        identity_file=POSTGRES_SSH_TUNNEL_KEY,
    ):
        
        """
        
        Creates a database call object for executing queries
        
        """
        url = URL.create(
                    drivername="postgresql+psycopg2",
                    username=POSTGRES_USER,
                    password=POSTGRES_PASSWORD,
                    host="127.0.0.1",
                    port=local_port,
                    database=POSTGRES_DATABASE,
                )
        connection = connect_to_db(url)
        call = DatabaseCall(connection=connection)

        # Builds the dlt resources to load
        resources = [
            DatabaseResource(
                object=object,
                call=call,
                write_disposition="append",
                incremental_attribute="updated",
                incremental_initial_value=datetime(1970, 1, 1, tzinfo=timezone.utc),
                table_name_suffix="__incr_updated",
                schema="public",
            ).create_resource()

            for object in objects
        ]

        """
        
        Builds destination environment variables
        - environment variables have the format:
            - {PIPELINE_NAME}__DESTINATION__{DESTINATION_TYPE}__{CONFIGURATION_KEY}
        - https://dlthub.com/docs/general-usage/destination#configure-a-destination
        
        """
        pipeline_name="example_postgres_to_s3"

        destination_env = {
            f"{pipeline_name.upper()}__DESTINATION__FILESYSTEM__BUCKET_URL": S3_PIPELINE_STAGING_BUCKET_URL,
            f"{pipeline_name.upper()}__DESTINATION__CREDENTIALS__ENDPOINT_URL": S3_PIPELINE_STAGING_ENDPOINT_URL,
            f"{pipeline_name.upper()}__DESTINATION__CREDENTIALS__AWS_ACCESS_KEY_ID": S3_PIPELINE_STAGING_ACCESS_KEY,
            f"{pipeline_name.upper()}__DESTINATION__CREDENTIALS__AWS_SECRET_ACCESS_KEY": S3_PIPELINE_STAGING_SECRET_ACCESS_KEY,
            f"{pipeline_name.upper()}__DESTINATION__CREDENTIALS__AWS_DEFAULT_REGION": S3_PIPELINE_STAGING_REGION,
        }

        """
        
        Builds the pipeline and loads resources/destination variables
        
        """
        pipeline = Pipeline(
            resources=resources,
            pipeline_name=pipeline_name,
            destination="filesystem",
            dataset="example_postgres_to_s3",
            **destination_env,
        )

        """

        Runs the pipeline and prints the output
        
        """
        try:
            pipeline.run_pipeline()
            pipeline.print_output()

        except Exception as e:
            raise e
        finally:
            connection.dispose()
