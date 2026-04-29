import os

import pytest

os.environ.setdefault("SNOWFLAKE_ACCOUNT", "test_account")
os.environ.setdefault("SNOWFLAKE_USER", "test_user")
os.environ.setdefault("SNOWFLAKE_PASSWORD", "test_password")

from dagster_snowflake_tutorial.definitions import defs


def test_definitions_can_load():
    assert defs is not None


def test_all_expected_asset_keys_present():
    keys = {k.to_user_string() for k in defs.get_all_asset_keys()}
    expected = {"taxi_trips", "stg_taxi_trips", "trip_metrics"}
    assert expected.issubset(keys), f"Missing assets: {expected - keys}"


def test_snowflake_resource_is_configured():
    resource_defs = defs.resources
    assert "snowflake" in resource_defs, "snowflake resource not found in definitions"


def test_no_duplicate_asset_keys():
    all_keys = [k.to_user_string() for k in defs.get_all_asset_keys()]
    assert len(all_keys) == len(set(all_keys)), f"Duplicate asset keys found: {all_keys}"
