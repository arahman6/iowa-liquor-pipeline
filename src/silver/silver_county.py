from pyspark.sql import DataFrame
from pyspark.sql.functions import col, max, to_date, row_number
from pyspark.sql.window import Window

class SilverCounty:
    def __init__(self, silver_path: str):
        """
        Initializes the Silver County class.

        :param silver_path: Path to store the Silver layer county data
        """
        self.silver_path = silver_path

    def transform_data(self, df: DataFrame):
        """
        Cleans and extracts county data.

        :param df: DataFrame from the Bronze layer
        :return: Transformed DataFrame
        """
        print("Transforming County data...")

        df_county = df.where(col("County Number").isNotNull()).groupBy(
            col("County Number").cast("int").alias("ID_COUNTY"), 
            col("County")
        ).agg(
            max(to_date(col("Date"), "yyyy-MM-dd")).alias("Latest_Date")
        )

        window_spec = Window.partitionBy("ID_COUNTY").orderBy(col("Latest_Date").desc())
        df_county_final = df_county.withColumn("rn", row_number().over(window_spec))\
            .filter(col("rn") == 1)\
            .select("ID_COUNTY", "County")

        df_county_final.write.mode("overwrite").parquet(f"{self.silver_path}/county.parquet")

        print("County data transformed and saved to Silver layer.")
        return df_county_final
