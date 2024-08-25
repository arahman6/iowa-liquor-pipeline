# src/gold/gold_vendor.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import sum, col

class GoldVendor:
    def __init__(self, gold_path: str):
        """
        Initializes the Gold Vendor class.

        :param gold_path: Path to store the Gold layer vendor data
        """
        self.gold_path = gold_path

    def aggregate_vendor_sales(self, df: DataFrame):
        """
        Aggregates total sales by vendor.

        :param df: DataFrame from the Silver layer
        :return: Aggregated DataFrame
        """
        print("Aggregating Vendor data...")

        df_vendor_agg = df.groupBy("ID_VENDOR").agg(
            sum("Sale_Dollars").alias("Total_Sales"),
            sum("Bottles_Sold").alias("Total_Bottles_Sold"),
            sum("Volume_Sold_Liters").alias("Total_Volume_Sold_Liters"),
            sum("Volume_Sold_Gallons").alias("Total_Volume_Sold_Gallons")
        )

        df_vendor_agg.write.mode("overwrite").parquet(f"{self.gold_path}/vendor.parquet")

        print("Vendor data aggregated and saved to Gold layer.")
        return df_vendor_agg
