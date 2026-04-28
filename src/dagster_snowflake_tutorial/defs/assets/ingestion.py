import dagster as dg
from dagster_snowflake import SnowflakeResource


@dg.asset(
    group_name="raw",
    description="Load January 2023 NYC yellow taxi trips from S3 into Snowflake",
    kinds={"snowflake", "python"},
)
def taxi_trips(snowflake: SnowflakeResource) -> dg.MaterializeResult:
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS TAXI_DATA.RAW.TAXI_TRIPS (
                VendorID              INTEGER,
                tpep_pickup_datetime  TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count       FLOAT,
                trip_distance         FLOAT,
                RatecodeID            FLOAT,
                store_and_fwd_flag    STRING,
                PULocationID          INTEGER,
                DOLocationID          INTEGER,
                payment_type          INTEGER,
                fare_amount           FLOAT,
                extra                 FLOAT,
                mta_tax               FLOAT,
                tip_amount            FLOAT,
                tolls_amount          FLOAT,
                improvement_surcharge FLOAT,
                total_amount          FLOAT,
                congestion_surcharge  FLOAT,
                airport_fee           FLOAT
            )
        """)

        cursor.execute("TRUNCATE TABLE TAXI_DATA.RAW.TAXI_TRIPS")

        cursor.execute("""
            COPY INTO TAXI_DATA.RAW.TAXI_TRIPS
            FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet'
            FILE_FORMAT = (
                TYPE                = PARQUET
                SNAPPY_COMPRESSION  = TRUE
            )
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR             = ABORT_STATEMENT
        """)

        cursor.execute("SELECT COUNT(*) FROM TAXI_DATA.RAW.TAXI_TRIPS")
        rows_loaded = cursor.fetchone()[0]

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(rows_loaded),
            "source_url": dg.MetadataValue.url(
                "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
            ),
        }
    )
