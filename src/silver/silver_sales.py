from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date

class SilverSales:
    def __init__(self, silver_path: str):
        """
        Initializes the Silver Sales class.

        :param silver_path: Path to store the Silver layer sales data
        """
        self.silver_path = silver_path

    def transform_data(self, df: DataFrame):
        """
        Cleans and transforms the sales data.

        :param df: DataFrame from the Bronze layer
        :return: Transformed DataFrame
        """
        print("Transforming Sales data...")

        df_transformed = df \
            .withColumnRenamed('invoice_line_no', 'Invoice_Item_Number') \
            .withColumnRenamed('date', 'Date') \
            .withColumn('Date', to_date(col('Date'), 'yyyy-MM-dd\'T\'HH:mm:ss.SSS')) \
            .withColumnRenamed('store', 'Store_Number') \
            .withColumnRenamed('sale_dollars', 'Sale_Dollars') \
            .withColumnRenamed('sale_liters', 'Volume_Sold_Liters') \
            .withColumnRenamed('sale_gallons', 'Volume_Sold_Gallons') \
            .drop('store_location')

        df_transformed.write.partitionBy("Date").mode("overwrite").parquet(f"{self.silver_path}/sales.parquet")

        print("Sales data transformed and saved to Silver layer.")
        return df_transformed
