from pathlib import Path

import psycopg
from pyspark.sql import SparkSession
from pyspark.sql.functions import max
from config import SOURCE_DB, TARGET_DB, POSTGRES

# ------------------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------------------

PROJECT_DIR = Path(__file__).parent

JAR_PATH = PROJECT_DIR / "jars" / "postgresql-42.7.12.jar"

WATERMARK_FILE = PROJECT_DIR / "data" / "watermark.txt"

# ------------------------------------------------------------------------------
# Spark Session
# ------------------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Incremental_Load")
    .master("local[*]")
    .config("spark.jars", str(JAR_PATH))
    .getOrCreate()
)

# ------------------------------------------------------------------------------
# Database Configuration
# ------------------------------------------------------------------------------

# It's in config file

# ------------------------------------------------------------------------------
# Watermark
# ------------------------------------------------------------------------------

def get_watermark():

    if not WATERMARK_FILE.exists():
        return None

    watermark = WATERMARK_FILE.read_text().strip()

    if watermark == "":
        return None

    return watermark


def save_watermark(watermark):

    WATERMARK_FILE.write_text(str(watermark))

    print("\nWatermark Updated Successfully.")


# ------------------------------------------------------------------------------
# Build Source Query
# ------------------------------------------------------------------------------

def get_source_query(watermark):

    if watermark is None:

        print("\nFirst Run -> Full Load\n")

        return SOURCE_DB["table"]

    print(f"\nCurrent Watermark : {watermark}\n")

    return f"""
    (
        SELECT *
        FROM {SOURCE_DB["table"]}
        WHERE updated_at > '{watermark}'
    ) AS customers
    """


# ------------------------------------------------------------------------------
# Read Source
# ------------------------------------------------------------------------------

def read_source(source_query):

    df = (
        spark.read
        .format("jdbc")
        .option("url", SOURCE_DB["url"])
        .option("dbtable", source_query)
        .option("user", SOURCE_DB["user"])
        .option("password", SOURCE_DB["password"])
        .option("driver", SOURCE_DB["driver"])
        .load()
    )

    print("\nSource Data\n")

    df.show()

    return df


# ------------------------------------------------------------------------------
# Write Staging
# ------------------------------------------------------------------------------

def write_target(df):

    (
        df.write
        .format("jdbc")
        .option("url", TARGET_DB["url"])
        .option("dbtable", TARGET_DB["table"])
        .option("user", TARGET_DB["user"])
        .option("password", TARGET_DB["password"])
        .option("driver", TARGET_DB["driver"])
        .mode("append")
        .save()
    )

    print("\nLoaded into Staging Table.")


# ------------------------------------------------------------------------------
# Watermark
# ------------------------------------------------------------------------------

def get_latest_watermark(df):

    return (
        df.agg(
            max("updated_at").alias("watermark")
        )
        .collect()[0]["watermark"]
    )


# ------------------------------------------------------------------------------
# Execute SQL
# ------------------------------------------------------------------------------

def execute_sql(sql):

    with psycopg.connect(
        host=POSTGRES["host"],
        port=POSTGRES["port"],
        dbname=POSTGRES["dbname"],
        user=POSTGRES["user"],
        password=POSTGRES["password"]
    ) as conn:

        with conn.cursor() as cur:

            cur.execute(sql)

        conn.commit()


# ------------------------------------------------------------------------------
# Merge
# ------------------------------------------------------------------------------

def merge_staging_to_target():

    sql = """
    MERGE INTO customers AS target

    USING stg_customers AS source

    ON target.customer_id = source.customer_id

    WHEN MATCHED THEN

        UPDATE SET

            customer_name = source.customer_name,

            city = source.city,

            updated_at = source.updated_at

    WHEN NOT MATCHED THEN

        INSERT
        (
            customer_id,
            customer_name,
            city,
            updated_at
        )

        VALUES
        (
            source.customer_id,
            source.customer_name,
            source.city,
            source.updated_at
        );
    """

    execute_sql(sql)

    print("\nMerge Completed.")


# ------------------------------------------------------------------------------
# Truncate Staging
# ------------------------------------------------------------------------------

def truncate_staging():

    execute_sql(
        """
        TRUNCATE TABLE stg_customers;
        """
    )

    print("Staging Table Truncated.")


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

def main():

    watermark = get_watermark()

    source_query = get_source_query(watermark)

    df = read_source(source_query)

    if df.isEmpty():

        print("\nNo New Records Found.")

        spark.stop()

        return

    write_target(df)

    merge_staging_to_target()

    latest_watermark = get_latest_watermark(df)

    print(f"\nLatest Watermark : {latest_watermark}")

    if latest_watermark is not None:

        save_watermark(latest_watermark)

    truncate_staging()

    spark.stop()


if __name__ == "__main__":
    main()