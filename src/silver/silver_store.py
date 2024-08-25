# src/silver/silver_store.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

class SilverStore:
    def __init__(self, silver_path: str):
        """
        Initializes the Silver Store class.

        :param silver_path: Path to store the Silver layer store data
        """
        self.silver_path = silver_path

    def transform_data(self, df: DataFrame):
        """
        Cleans and transforms the store data.

        :param df: DataFrame from the Bronze layer
        :return: Transformed DataFrame
        """
        print("Transforming Store data...")

        df_transformed = df \
            .withColumnRenamed('store', 'Store_Number') \
            .withColumnRenamed('name', 'Store_Name') \
            .withColumnRenamed('address', 'Store_Address') \
            .withColumnRenamed('city', 'Store_City') \
            .withColumnRenamed('zipcode', 'Store_Zip_Code') \
            .dropDuplicates(['Store_Number'])

        df_transformed.write.mode("overwrite").parquet(f"{self.silver_path}/store.parquet")

        print("Store data transformed and saved to Silver layer.")
        return df_transformed
