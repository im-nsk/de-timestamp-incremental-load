from pathlib import Path
from pyspark.sql import SparkSession
from config import SOURCE_DB

PROJECT_DIR = Path(__file__).parent

POSTGRES_JAR = PROJECT_DIR / "jars" / "postgresql-42.7.12.jar"
SNOWFLAKE_JDBC = PROJECT_DIR / "jars" / "snowflake-jdbc-4.3.4.jar"
SNOWFLAKE_CONNECTOR = PROJECT_DIR / "jars" / "spark-snowflake_2.13-3.2.2-spark_4.1.jar"

spark = (
    SparkSession.builder
    .appName("Incremental_Load_Snowflake")
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

def read_source():
    df = (
        spark.read
        .format("jdbc")
        .option("url", SOURCE_DB["url"])
        .option("dbtable", SOURCE_DB["table"])
        .option("user", SOURCE_DB["user"])
        .option("password", SOURCE_DB["password"])
        .option("driver", SOURCE_DB["driver"])
        .load()
    )

    df.show()
    return df

def main():

    df = read_source()
    print(df.count())
    spark.stop()

if __name__ == "__main__":
    main()