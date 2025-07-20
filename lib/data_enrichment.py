from pyspark.sql.functions import col, lit, current_timestamp



class data_enrichment:
    def __init__(self,bronze_db,bronze_table,silver_db,silver_table,spark):
        self.spark = spark
        self.bronze_db=bronze_db
        self.bronze_table=bronze_table
        self.silver_db=silver_db
        self.silver_table=silver_table
        self.control_table = "control_table"
        self.latest_timestamp = self.get_latest_audit_timestamp(spark,self.silver_db, self.silver_table, self.control_table)
        self.process_silver_table(spark, self.bronze_db, self.bronze_table, self.silver_db, self.silver_table, self.latest_timestamp, self.control_table)


    @staticmethod
    def get_latest_audit_timestamp(spark,silver_db, silver_table, control_table):

        control_df = spark.table(f"{silver_db}.{control_table}")

        filtered = control_df.filter(control_df.table_name == silver_table)

        if filtered.limit(1).count() > 0:
            # latest_ts = filtered.select("last_updated_timestamp").first()["last_updated_timestamp"]
            latest_ts = filtered.select("last_updated_timestamp").collect()[0]["last_updated_timestamp"]

        else:
            latest_ts = '1970-01-01'

        return latest_ts

    @staticmethod
    def process_silver_table(spark, bronze_db, bronze_table, silver_db, silver_table, latest_timestamp,control_table):
        bronze_view =  bronze_table + "_bronze_to_silver_view"
        df_bronze = spark.table(f"{bronze_db}.{bronze_view}")

        df_bronze_filtered = df_bronze.filter(col("audit_created_timestamp") >= latest_timestamp)

        max_audit_timestamp = df_bronze_filtered.agg({"audit_created_timestamp": "max"}).collect()[0][0]

        folder_path = silver_table.split("_")[1]
        print(folder_path)

        location = f"/Users/prasad.hegde/Desktop/LearningProject/data-warehouse-Learning/tables/silver/{folder_path}"

        print(f"updating silver table :- {silver_table}")

        try:
            df_bronze_filtered.write.format("delta").option("mergeSchema", "true").mode("append").save(location)

        except Exception as e:
            print(f"Exception occurred while writing to silver table: {e}")

        spark.sql(f"delete from {silver_db}.{control_table}")

        update_query = f"insert into {silver_db}.{control_table} (table_name, last_updated_timestamp) values ('{silver_table}', '{max_audit_timestamp}')"

        print(update_query)










