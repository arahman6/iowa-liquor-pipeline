from pyspark.sql import SparkSession

class BronzeIngestion:
    def __init__(self, spark: SparkSession, source_path: str, bronze_path: str):
        """
        Initializes the Bronze Ingestion class.

        :param spark: SparkSession object
        :param source_path: Path to the raw JSON data source
        :param bronze_path: Path to store the Bronze layer data
        """
        self.spark = spark
        self.source_path = source_path
        self.bronze_path = bronze_path

    def ingest_data(self):
        """
        Reads raw JSON data from the source and writes it to the Bronze layer in Parquet format.
        
        :return: DataFrame containing the ingested data
        """
        print(f"Ingesting data from {self.source_path} to Bronze layer...")

        # Read raw JSON data
        df = self.spark.read.option("multiline", "true").json(self.source_path)

        # Save the data in Parquet format, partitioned by Date
        df.write.partitionBy("Date").mode("overwrite").parquet(self.bronze_path)

        print(f"Data successfully ingested to {self.bronze_path}")
        return df
