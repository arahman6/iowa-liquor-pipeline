# src/silver/silver_vendor.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, max, to_date, row_number, lpad
from pyspark.sql.window import Window

class SilverVendor:
    def __init__(self, silver_path: str):
        """
        Initializes the Silver Vendor class.

        :param silver_path: Path to store the Silver layer vendor data
        """
        self.silver_path = silver_path

    def transform_data(self, df: DataFrame):
        """
        Cleans and extracts vendor data.

        :param df: DataFrame from the Bronze layer
        :return: Transformed DataFrame
        """
        print("Transforming Vendor data...")

        df_vendor = df.withColumn("Vendor_Number_Str", lpad(col("Vendor Number").cast("string"), 4, '0'))

        df_vendor_latest = df_vendor.where(col("Vendor Number").isNotNull()).groupBy(
            col("Vendor_Number_Str").alias("ID_VENDOR"),  # Padded Vendor Number
            col("Vendor Name")
        ).agg(
            max(to_date(col("Date"), "yyyy-MM-dd")).alias("Latest_Date")
        )

        window_spec = Window.partitionBy("ID_VENDOR").orderBy(col("Latest_Date").desc())
        df_vendor_final = df_vendor_latest.withColumn("rn", row_number().over(window_spec))\
            .filter(col("rn") == 1)\
            .select("ID_VENDOR", col("Vendor Name").alias("Vendor_Name"))

        df_vendor_final.write.mode("overwrite").parquet(f"{self.silver_path}/vendor.parquet")

        print("Vendor data transformed and saved to Silver layer.")
        return df_vendor_final
