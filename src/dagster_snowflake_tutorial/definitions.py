from pathlib import Path

import dagster as dg
from dagster import definitions, load_from_defs_folder
from dagster_snowflake import SnowflakeResource


@definitions
def defs():
    loaded = load_from_defs_folder(path_within_project=Path(__file__).parent)
    return loaded.with_resources(
        {
            "snowflake": SnowflakeResource(
                account=dg.EnvVar("SNOWFLAKE_ACCOUNT"),
                user=dg.EnvVar("SNOWFLAKE_USER"),
                password=dg.EnvVar("SNOWFLAKE_PASSWORD"),
                warehouse="TAXI_WH",
                database="TAXI_DATA",
                schema_="RAW",
                role="ACCOUNTADMIN",
            ),
        }
    )
