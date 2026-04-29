from unittest.mock import MagicMock, patch

import dagster as dg

from dagster_snowflake_tutorial.defs.assets.ingestion import taxi_trips


def _make_mock_snowflake(row_count=100):
    mock_resource = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (row_count,)
    mock_conn.cursor.return_value = mock_cursor
    mock_resource.get_connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_resource.get_connection.return_value.__exit__ = MagicMock(return_value=False)
    return mock_resource, mock_cursor


def test_taxi_trips_executes_correct_sql_sequence():
    mock_sf, mock_cursor = _make_mock_snowflake(row_count=3000000)
    result = taxi_trips(snowflake=mock_sf)

    calls = [call.args[0].strip() for call in mock_cursor.execute.call_args_list]
    assert any("CREATE TABLE" in c for c in calls), "Should create table"
    assert any("TRUNCATE" in c for c in calls), "Should truncate before load"
    assert any("COPY INTO" in c for c in calls), "Should copy data from source"
    assert any("SELECT COUNT" in c for c in calls), "Should count loaded rows"


def test_taxi_trips_returns_materialize_result():
    mock_sf, _ = _make_mock_snowflake(row_count=50000)
    result = taxi_trips(snowflake=mock_sf)

    assert isinstance(result, dg.MaterializeResult)
    assert "rows_loaded" in result.metadata
    assert result.metadata["rows_loaded"].value == 50000


def test_taxi_trips_returns_source_url_metadata():
    mock_sf, _ = _make_mock_snowflake(row_count=1)
    result = taxi_trips(snowflake=mock_sf)

    assert "source_url" in result.metadata
    assert "yellow_tripdata_2023-01" in result.metadata["source_url"].value


def test_taxi_trips_table_has_expected_columns():
    mock_sf, mock_cursor = _make_mock_snowflake()
    taxi_trips(snowflake=mock_sf)

    create_call = [c.args[0] for c in mock_cursor.execute.call_args_list if "CREATE TABLE" in c.args[0]][0]
    expected_columns = [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "fare_amount",
        "tip_amount", "total_amount", "PULocationID", "DOLocationID",
    ]
    for col in expected_columns:
        assert col in create_call, f"Missing column: {col}"
