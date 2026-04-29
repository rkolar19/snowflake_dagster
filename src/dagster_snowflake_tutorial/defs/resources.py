import os

import dagster as dg
from dagster_snowflake import SnowflakeResource

snowflake = SnowflakeResource(
    account=dg.EnvVar("SNOWFLAKE_ACCOUNT"),
    user=dg.EnvVar("SNOWFLAKE_USER"),
    password=dg.EnvVar("SNOWFLAKE_PASSWORD"),
    warehouse="TAXI_WH",
    database="TAXI_DATA",
    schema_="RAW",
    role="ACCOUNTADMIN",
)

taxi_pipeline_job = dg.define_asset_job(
    name="taxi_pipeline_daily",
    selection=dg.AssetSelection.all(),
    description="Full refresh: ingest from S3, build staging view, build marts table",
)

daily_schedule = dg.ScheduleDefinition(
    name="taxi_pipeline_daily_schedule",
    job=taxi_pipeline_job,
    cron_schedule="0 6 * * *",
)
