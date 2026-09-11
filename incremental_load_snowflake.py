from pathlib import Path
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import max as spark_max

from config import SOURCE_DB, SNOWFLAKE


# --------------------------------------------------
# Project paths
# --------------------------------------------------

PROJECT_DIR = Path(__file__).resolve().parent

PRIVATE_KEY_PATH = (
    Path.home()
    / "snowflake_keys"
    / "rsa_key.p8"
)

WATERMARK_FILE = (
    PROJECT_DIR
    / "data"
    / "watermark_sf.txt"
)


# --------------------------------------------------
# JAR files
# --------------------------------------------------

POSTGRES_JAR = (
    PROJECT_DIR
    / "jars"
    / "postgresql-42.7.12.jar"
)

SNOWFLAKE_JDBC = (
    PROJECT_DIR
    / "jars"
    / "snowflake-jdbc-4.3.4.jar"
)

SNOWFLAKE_CONNECTOR = (
    PROJECT_DIR
    / "jars"
    / "spark-snowflake_2.13-3.2.2-spark_4.1.jar"
)


# --------------------------------------------------
# Snowflake private key
# --------------------------------------------------

def load_snowflake_private_key():
    with open(PRIVATE_KEY_PATH, "r") as key_file:
        pem_private_key = key_file.read()

    pem_private_key = re.sub(
        r"-----BEGIN PRIVATE KEY-----",
        "",
        pem_private_key
    )

    pem_private_key = re.sub(
        r"-----END PRIVATE KEY-----",
        "",
        pem_private_key
    )

    pem_private_key = (
        pem_private_key
        .replace("\n", "")
        .strip()
    )

    SNOWFLAKE["pem_private_key"] = pem_private_key


# --------------------------------------------------
# Read watermark
# --------------------------------------------------

def read_watermark():

    with open(WATERMARK_FILE, "r") as file:
        watermark = file.read().strip()

    if not watermark:
        watermark = "1900-01-01 00:00:00"

    print(f"Current watermark: {watermark}")

    return watermark


# --------------------------------------------------
# Create Spark session
# --------------------------------------------------

def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("PostgresToSnowflakeIncremental")
        .master("local[*]")
        .config(
            "spark.jars",
            ",".join([
                str(POSTGRES_JAR),
                str(SNOWFLAKE_JDBC),
                str(SNOWFLAKE_CONNECTOR)
            ])
        )
        .getOrCreate()
    )

    return spark


# --------------------------------------------------
# Read incremental data from PostgreSQL
# --------------------------------------------------

def read_incremental_data(spark, watermark):

    query = f"""
    (
        SELECT
            customer_id,
            customer_name,
            city,
            updated_at
        FROM customers
        WHERE updated_at > '{watermark}'
    ) AS source_data
    """

    df = (
        spark.read
        .format("jdbc")
        .option("url", SOURCE_DB["url"])
        .option("dbtable", query)
        .option("user", SOURCE_DB["user"])
        .option("password", SOURCE_DB["password"])
        .option("driver", SOURCE_DB["driver"])
        .load()
    )

    row_count = df.count()

    print(f"Incremental rows read: {row_count}")

    if row_count > 0:
        df.show()

    return df


# --------------------------------------------------
# Write incremental data to Snowflake staging
# --------------------------------------------------

def write_to_staging(df):

    staging_table = "CUSTOMERS_STAGING"

    merge_sql = """
    MERGE INTO DE_PROJECTS.BRONZE.CUSTOMERS AS target
    USING DE_PROJECTS.BRONZE.CUSTOMERS_STAGING AS source

    ON target.CUSTOMER_ID = source.CUSTOMER_ID

    WHEN MATCHED THEN
        UPDATE SET
            target.CUSTOMER_NAME = source.CUSTOMER_NAME,
            target.CITY = source.CITY,
            target.UPDATED_AT = source.UPDATED_AT

    WHEN NOT MATCHED THEN
        INSERT (
            CUSTOMER_ID,
            CUSTOMER_NAME,
            CITY,
            UPDATED_AT
        )
        VALUES (
            source.CUSTOMER_ID,
            source.CUSTOMER_NAME,
            source.CITY,
            source.UPDATED_AT
        )
    """

    (
        df.write
        .format("snowflake")
        .options(**SNOWFLAKE)
        .option("dbtable", staging_table)
        .option("postactions", merge_sql)
        .mode("overwrite")
        .save()
    )

    print("Incremental data loaded and MERGE completed.")


# --------------------------------------------------
# Calculate new watermark
# --------------------------------------------------

def get_new_watermark(df):

    new_watermark = (
        df
        .select(
            spark_max("updated_at").alias("max_updated_at")
        )
        .collect()[0]["max_updated_at"]
    )

    print(f"New watermark: {new_watermark}")

    return new_watermark


# --------------------------------------------------
# Update watermark
# --------------------------------------------------

def update_watermark(new_watermark):

    with open(WATERMARK_FILE, "w") as file:
        file.write(str(new_watermark))

    print(f"Watermark updated to: {new_watermark}")


# --------------------------------------------------
# Main pipeline
# --------------------------------------------------

def main():

    load_snowflake_private_key()

    watermark = read_watermark()

    spark = create_spark_session()

    try:

        df = read_incremental_data(
            spark,
            watermark
        )

        if df.rdd.isEmpty():
            print("No new data found.")
            return

        new_watermark = get_new_watermark(df)

        write_to_staging(df)

        update_watermark(new_watermark)

        print("Pipeline completed successfully.")

    finally:
        spark.stop()


# --------------------------------------------------
# Entry point
# --------------------------------------------------

if __name__ == "__main__":
    main()