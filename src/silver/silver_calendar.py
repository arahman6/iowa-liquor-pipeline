from pyspark.sql import DataFrame
from pyspark.sql.functions import to_date, col

class SilverCalendar:
    def __init__(self, silver_path: str):
        """
        Initializes the Silver Calendar class.

        :param silver_path: Path to store the Silver layer calendar data
        """
        self.silver_path = silver_path

    def transform_data(self, df: DataFrame):
        """
        Extracts distinct calendar dates.

        :param df: DataFrame from the Bronze layer
        :return: Transformed DataFrame
        """
        print("Transforming Calendar data...")

        df_calendar = df.where(col("Date").isNotNull()).select(
            col("Date").alias("ID_CALENDAR"),
            to_date(col("Date"), "yyyy-MM-dd").alias("Date")
        ).distinct()

        df_calendar.write.mode("overwrite").parquet(f"{self.silver_path}/calendar.parquet")

        print("Calendar data transformed and saved to Silver layer.")
        return df_calendar
