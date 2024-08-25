from pyspark.sql import DataFrame
from pyspark.sql.functions import sum, col

class GoldCalendar:
    def __init__(self, gold_path: str):
        """
        Initializes the Gold Calendar class.

        :param gold_path: Path to store the Gold layer calendar data
        """
        self.gold_path = gold_path

    def aggregate_calendar_sales(self, df: DataFrame):
        """
        Aggregates total sales by date.

        :param df: DataFrame from the Silver layer
        :return: Aggregated DataFrame
        """
        print("Aggregating Calendar data...")

        df_calendar_agg = df.groupBy("ID_CALENDAR").agg(
            sum("Sale_Dollars").alias("Total_Sales"),
            sum("Bottles_Sold").alias("Total_Bottles_Sold"),
            sum("Volume_Sold_Liters").alias("Total_Volume_Sold_Liters"),
            sum("Volume_Sold_Gallons").alias("Total_Volume_Sold_Gallons")
        )

        df_calendar_agg.write.mode("overwrite").parquet(f"{self.gold_path}/calendar.parquet")

        print("Calendar data aggregated and saved to Gold layer.")
        return df_calendar_agg
