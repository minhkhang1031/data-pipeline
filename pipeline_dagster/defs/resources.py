import os
from pathlib import Path

import boto3
import dagster as dg
from dagster_dbt import DbtCliResource
from dagster_duckdb import DuckDBResource


DBT_PROJECT_DIR = (
    Path(__file__)
    .joinpath("..", "..", "..", "transformation")
    .resolve()
)


database_resource = DuckDBResource(
    database=dg.EnvVar("DUCKDB_DATABASE"),
)


if os.getenv("DAGSTER_ENVIRONMENT") == "prod":
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )

    smart_open_config = {
        "client": session.client("s3"),
    }
else:
    smart_open_config = {}


dbt_resource = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
)

# Exposed as top-level variables so dg.load_from_defs_folder can automatically collect them
database = database_resource
dbt = dbt_resource
