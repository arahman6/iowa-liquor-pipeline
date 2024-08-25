from pyspark.sql import DataFrame
from pyspark.sql.functions import sum, col

class GoldSales:
    def __init__(self, gold_path: str):
        """
        Initializes the Gold Sales class.

        :param gold_path: Path to store the Gold layer sales data
        """
        self.gold_path = gold_path

    def aggregate_sales(self, df: DataFrame):
        """
        Aggregates total sales at the store level.

        :param df: DataFrame from the Silver layer
        :return: Aggregated DataFrame
        """
        print("Aggregating Sales data...")

        df_agg = df.groupBy("Store_Number").agg(
            sum("Sale_Dollars").alias("Total_Sales"),
            sum("Bottles_Sold").alias("Total_Bottles_Sold"),
            sum("Volume_Sold_Liters").alias("Total_Volume_Sold_Liters"),
            sum("Volume_Sold_Gallons").alias("Total_Volume_Sold_Gallons")
        )

        df_agg.write.mode("overwrite").parquet(f"{self.gold_path}/sales.parquet")

        print("Sales data aggregated and saved to Gold layer.")
        return df_agg
