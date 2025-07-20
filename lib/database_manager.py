

class DatabaseManager:
    def __init__(self, spark):
        self.spark = spark
        self.create_databases()

    def create_databases(self):
        self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze_db")
        self.spark.sql("CREATE DATABASE IF NOT EXISTS silver_db")
        self.spark.sql("CREATE DATABASE IF NOT EXISTS gold_db")
        self.spark.sql("CREATE DATABASE IF NOT EXISTS control_db")



