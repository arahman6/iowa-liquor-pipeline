from pyspark.sql import DataFrame
from pyspark.sql.functions import countDistinct, col

class GoldStore:
    def __init__(self, gold_path: str):
        """
        Initializes the Gold Store class.

        :param gold_path: Path to store the Gold layer store data
        """
        self.gold_path = gold_path

    def aggregate_store_data(self, df: DataFrame):
        """
        Aggregates store data including unique store count per city.

        :param df: DataFrame from the Silver layer
        :return: Aggregated DataFrame
        """
        print("Aggregating Store data...")

        df_store_agg = df.groupBy("Store_City").agg(
            countDistinct("Store_Number").alias("Total_Stores")
        )

        df_store_agg.write.mode("overwrite").parquet(f"{self.gold_path}/store.parquet")

        print("Store data aggregated and saved to Gold layer.")
        return df_store_agg
