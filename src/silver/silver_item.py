from pyspark.sql import DataFrame
from pyspark.sql.functions import col, max, to_date, row_number
from pyspark.sql.window import Window

class SilverItem:
    def __init__(self, silver_path: str):
        """
        Initializes the Silver Item class.

        :param silver_path: Path to store the Silver layer item data
        """
        self.silver_path = silver_path

    def transform_data(self, df: DataFrame):
        """
        Cleans and extracts item data.

        :param df: DataFrame from the Bronze layer
        :return: Transformed DataFrame
        """
        print("Transforming Item data...")

        df_item = df.where(col("Item Number").isNotNull()).groupBy(
            col("Item Number").alias("ID_ITEM"), 
            col("Item Description"), 
            col("Pack"), 
            col("Bottle Volume (ml)"), 
            col("State Bottle Cost"), 
            col("State Bottle Retail")
        ).agg(
            max(to_date(col("Date"), "yyyy-MM-dd")).alias("Latest_Date")
        )

        window_spec = Window.partitionBy("ID_ITEM").orderBy(col("Latest_Date").desc())
        df_item_final = df_item.withColumn("rn", row_number().over(window_spec))\
            .filter(col("rn") == 1)\
            .select(
                col("ID_ITEM"),
                col("Item Description"),
                col("Pack").cast("bigint"),
                col("Bottle Volume (ml)").cast("bigint"),
                col("State Bottle Cost").cast("float"),
                col("State Bottle Retail").cast("float"),
                # Calculating Price Per Volume (Lt) Cost
                (col("State Bottle Cost") / (col("Bottle Volume (ml)") / 1000)).alias("Price_Per_Volume_Lt_Cost"),
                # Calculating Price Per Volume (Lt) Retail
                (col("State Bottle Retail") / (col("Bottle Volume (ml)") / 1000)).alias("Price_Per_Volume_Lt_Retail"),
                # Calculating Price Commission Per Volume (Lt)
                ((col("State Bottle Retail") - col("State Bottle Cost")) / (col("Bottle Volume (ml)") / 1000)).alias("Price_Commission_Per_Volume_Lt"),
                # Calculating Profit Margin
                ((col("State Bottle Retail") / col("State Bottle Cost")) - 1).alias("Profit_Margin")
            )

        df_item_final.write.mode("overwrite").parquet(f"{self.silver_path}/item.parquet")

        print("Item data transformed and saved to Silver layer.")
        return df_item_final
