from pathlib import Path
import re
from pyspark.sql import SparkSession
from config import SOURCE_DB, SNOWFLAKE

from cryptography.hazmat.primitives import serialization

PROJECT_DIR = Path(__file__).parent

PRIVATE_KEY_PATH = Path.home()/"snowflake_keys"/"rsa_key.p8"

# with open(PRIVATE_KEY_PATH, "rb") as key_file:
#     private_key = key_file.read()

# PRIVATE_KEY_PATH = Path.home() / "snowflake_keys" / "rsa_key.p8"

# with open(PRIVATE_KEY_PATH, "rb") as key_file:
#     private_key = serialization.load_pem_private_key(
#         key_file.read(),
#         password=None
#     )

# private_key_bytes = private_key.private_bytes(
#     encoding=serialization.Encoding.DER,
#     format=serialization.PrivateFormat.PKCS8,
#     encryption_algorithm=serialization.NoEncryption()
# )

# with open(PRIVATE_KEY_PATH, "rb") as key_file:
#     private_key = serialization.load_pem_private_key(
#         key_file.read(),
#         password=None
#     )

# pem_private_key = private_key.private_bytes(
#     encoding=serialization.Encoding.PEM,
#     format=serialization.PrivateFormat.PKCS8,
#     encryption_algorithm=serialization.NoEncryption()
# ).decode("UTF-8")

# pem_private_key = re.sub(
#     r"-*(BEGIN|END) PRIVATE KEY-*\n",
#     "",
#     pem_private_key
# ).replace("\n", "")

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

pem_private_key = pem_private_key.replace("\n", "").strip()


SNOWFLAKE["pem_private_key"] = pem_private_key

print("Key type:", type(pem_private_key))
print("Key length:", len(pem_private_key))
print("Starts with:", pem_private_key[:20])
print("Contains BEGIN:", "BEGIN" in pem_private_key)
print("Contains END:", "END" in pem_private_key)

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


def write_to_snowflake(df):
    (
        df.write
        .format("snowflake")
        .options(**SNOWFLAKE)
        .option("dbtable", "CUSTOMERS")
        .mode("overwrite")
        .save()
    )

    print("Loaded to Snowflake")

    


def main():

    df = read_source()
    print(df.count())

    write_to_snowflake(df)

    spark.stop()

if __name__ == "__main__":
    main()