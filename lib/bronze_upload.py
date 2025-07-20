import os
from pyspark.sql.functions import current_timestamp,year,month,dayofmonth,col,to_date,date_sub


class BronzeUpload:
    def __init__(self, spark=None):
        self.dataset_folder = "/Users/prasad.hegde/Desktop/LearningProject/data-warehouse-Learning/dataset/"
        self.spark = spark
        self.upload_to_bronze()

    def upload_to_bronze(self):

        print("uploading the file to bronze table")

        for folder in os.listdir(self.dataset_folder):
            if folder != ".DS_Store":
                folder_path = os.path.join(self.dataset_folder, folder)
                for file_name in os.listdir(folder_path):
                    if file_name.endswith(".csv"):
                        file_path = os.path.join(folder_path, file_name)
                        print(f"Processing file: {file_path}")
                        self.process_file(file_path, folder, self.spark)
                    elif file_name.endswith(".json"):
                        file_path = os.path.join(folder_path, file_name)
                        print(f"Processing file: {file_path}")
                        self.process_json_file(file_path, folder, self.spark)



    @staticmethod
    def process_file(file_path, folder, spark):
        print(f"Processing file: {file_path}")

        df = spark.read.format("csv").option("header", "true").load(file_path)

        df_audit_column = df.withColumns(
            {
                "audit_created_timestamp": to_date(current_timestamp(), "yyyy-MM-dd"),
                "year" : year(current_timestamp()),
                "month" : month(current_timestamp()),
                "day" : dayofmonth(current_timestamp())
            }
        )

        df_final = df_audit_column.select([col(c).cast("string").alias(c) for c in df_audit_column.columns])

        location = f"/Users/prasad.hegde/Desktop/LearningProject/data-warehouse-Learning/tables/bronze/{folder}/v1"

        df_final.write \
            .format("parquet") \
            .partitionBy("audit_created_timestamp") \
            .mode("append") \
            .save(location)

    @staticmethod
    def process_json_file(file_path, folder, spark):
        print(f"Processing file: {file_path}")

        df = spark.read.format("json").load(file_path)

        df_audit_column = df.withColumns(
            {
                "audit_created_timestamp": to_date(current_timestamp(), "yyyy-MM-dd"),
                "year": year(current_timestamp()),
                "month": month(current_timestamp()),
                "day": dayofmonth(current_timestamp())
            }
        )

        df_final = df_audit_column.select([col(c).cast("string").alias(c) for c in df_audit_column.columns])

        location = f"/Users/prasad.hegde/Desktop/LearningProject/data-warehouse-Learning/tables/bronze/{folder}/v1"

        df_final.write \
            .format("parquet") \
            .partitionBy("audit_created_timestamp") \
            .mode("append") \
            .save(location)









