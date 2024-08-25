from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number, max, to_date
from pyspark.sql.window import Window

class SilverCategory:
    def __init__(self, silver_path: str):
        """
        Initializes the Silver Category class.

        :param silver_path: Path to store the Silver layer category data
        """
        self.silver_path = silver_path

    def transform_data(self, df: DataFrame):
        """
        Cleans and transforms the category data.

        :param df: DataFrame from the Bronze layer
        :return: Transformed DataFrame
        """
        print("Transforming Category data...")

        df_category = df.where(col("Category").isNotNull()).groupBy(
            col("Category").cast("int").alias("ID_CATEGORY"),
            col("Category Name")
        ).agg(
            max(to_date(col("Date"), "yyyy-MM-dd")).alias("Latest_Date")
        )

        # Apply window function to get the latest category data
        window_spec = Window.partitionBy("ID_CATEGORY").orderBy(col("Latest_Date").desc())

        df_category_final = df_category.withColumn("rn", row_number().over(window_spec))\
            .filter(col("rn") == 1)\
            .select("ID_CATEGORY", col("Category Name").alias("Category_Name"))

        df_category_final.write.mode("overwrite").parquet(f"{self.silver_path}/category.parquet")

        print("Category data transformed and saved to Silver layer.")
        return df_category_final
