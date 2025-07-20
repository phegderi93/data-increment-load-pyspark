from lib.spark_session import get_spark_session, get_spark_session_local
from lib.database_manager import DatabaseManager
from lib.bronze_upload import BronzeUpload
from lib.create_tables import create_bronze_table, create_silver_table,bronze_to_silver_view
from lib.data_enrichment import data_enrichment



if __name__ == "__main__":


    spark = get_spark_session()
    DatabaseManager(spark)
    # BronzeUpload(spark)
    #
    create_bronze_table(spark)
    bronze_to_silver_view(spark)
    #
    create_silver_table(spark)

    spark.sql("select * from silver_db.control_table").show()

    data_enrichment("bronze_db","bronze_customer_v1","silver_db","silver_customer",spark)

    spark.sql("select * from silver_db.control_table").show()

    spark.sql("select * from silver_db.silver_customer").show()























