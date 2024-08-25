from pyspark.sql import DataFrame
from pyspark.sql.functions import sum, col

class GoldCategory:
    def __init__(self, gold_path: str):
        """
        Initializes the Gold Category class.

        :param gold_path: Path to store the Gold layer category data
        """
        self.gold_path = gold_path

    def aggregate_category_sales(self, df: DataFrame):
        """
        Aggregates total sales by category.

        :param df: DataFrame from the Silver layer
        :return: Aggregated DataFrame
        """
        print("Aggregating Category data...")

        df_category_agg = df.groupBy("ID_CATEGORY").agg(
            sum("Sale_Dollars").alias("Total_Sales"),
            sum("Bottles_Sold").alias("Total_Bottles_Sold")
        )

        df_category_agg.write.mode("overwrite").parquet(f"{self.gold_path}/category.parquet")

        print("Category data aggregated and saved to Gold layer.")
        return df_category_agg
