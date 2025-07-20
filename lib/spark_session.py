from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark_session(appname="default"):
    builder = SparkSession.builder \
        .appName("DeltaTest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

def get_spark_session_local(appname="default"):
    spark = SparkSession.builder \
    .appName(appname) \
    .master("local[*]") \
    .getOrCreate()
    return spark
