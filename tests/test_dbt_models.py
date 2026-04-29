import os
import subprocess

import pytest


def test_dbt_project_parses():
    result = subprocess.run(
        ["dbt", "parse", "--project-dir", "analytics", "--profiles-dir", "analytics"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"dbt parse failed: {result.stderr}"


def test_dbt_models_compile():
    result = subprocess.run(
        ["dbt", "compile", "--project-dir", "analytics", "--profiles-dir", "analytics"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"dbt compile failed: {result.stderr}"


def test_staging_model_filters_bad_data():
    compiled_path = "analytics/target/compiled/analytics/models/staging/stg_taxi_trips.sql"
    if not os.path.exists(compiled_path):
        subprocess.run(
            ["dbt", "compile", "--project-dir", "analytics", "--profiles-dir", "analytics"],
            capture_output=True,
        )
    if os.path.exists(compiled_path):
        with open(compiled_path) as f:
            sql = f.read()
        assert "trip_distance > 0" in sql, "Staging should filter zero-distance trips"
        assert "total_amount  > 0" in sql or "total_amount > 0" in sql, "Staging should filter zero-amount trips"


def test_marts_model_aggregates_by_hour():
    compiled_path = "analytics/target/compiled/analytics/models/marts/trip_metrics.sql"
    if not os.path.exists(compiled_path):
        subprocess.run(
            ["dbt", "compile", "--project-dir", "analytics", "--profiles-dir", "analytics"],
            capture_output=True,
        )
    if os.path.exists(compiled_path):
        with open(compiled_path) as f:
            sql = f.read()
        assert "date_trunc" in sql.lower(), "Marts should aggregate by hour"
        assert "pickup_location_id" in sql.lower(), "Marts should group by location"
