from pyspark.sql import DataFrame
from pyspark.sql.functions import sum, col

class GoldCounty:
    def __init__(self, gold_path: str):
        """
        Initializes the Gold County class.

        :param gold_path: Path to store the Gold layer county data
        """
        self.gold_path = gold_path

    def aggregate_county_sales(self, df: DataFrame):
        """
        Aggregates total sales by county.

        :param df: DataFrame from the Silver layer
        :return: Aggregated DataFrame
        """
        print("Aggregating County data...")

        df_county_agg = df.groupBy("ID_COUNTY").agg(
            sum("Sale_Dollars").alias("Total_Sales"),
            sum("Bottles_Sold").alias("Total_Bottles_Sold"),
            sum("Volume_Sold_Liters").alias("Total_Volume_Sold_Liters"),
            sum("Volume_Sold_Gallons").alias("Total_Volume_Sold_Gallons")
        )

        df_county_agg.write.mode("overwrite").parquet(f"{self.gold_path}/county.parquet")

        print("County data aggregated and saved to Gold layer.")
        return df_county_agg
