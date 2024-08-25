import yaml
from pyspark.sql import SparkSession
from src.bronze.bronze_ingestion import BronzeIngestion
from src.silver.silver_sales import SilverSales
from src.silver.silver_store import SilverStore
from src.silver.silver_category import SilverCategory
from src.silver.silver_calendar import SilverCalendar
from src.silver.silver_county import SilverCounty
from src.silver.silver_vendor import SilverVendor
from src.silver.silver_item import SilverItem
from src.gold.gold_sales import GoldSales
from src.gold.gold_store import GoldStore
from src.gold.gold_category import GoldCategory
from src.gold.gold_calendar import GoldCalendar
from src.gold.gold_county import GoldCounty
from src.gold.gold_vendor import GoldVendor
from src.gold.gold_item import GoldItem

# Load configurations
with open("config/config.yaml", "r") as file:
    config = yaml.safe_load(file)

# Initialize SparkSession (Databricks provides `spark` by default)
spark = SparkSession.builder.appName("IowaLiquorPipeline").getOrCreate()

# Bronze Layer
bronze_ingestor = BronzeIngestion(spark, config["source_path"], config["bronze_path"])
df_bronze = bronze_ingestor.ingest_data()

# Silver Layer
silver_processors = {
    "sales": SilverSales(config["silver_path"]),
    "store": SilverStore(config["silver_path"]),
    "category": SilverCategory(config["silver_path"]),
    "calendar": SilverCalendar(config["silver_path"]),
    "county": SilverCounty(config["silver_path"]),
    "vendor": SilverVendor(config["silver_path"]),
    "item": SilverItem(config["silver_path"])
}

df_silver = {}
for table in config["tables"]:
    df_silver[table] = silver_processors[table].transform_data(df_bronze)

# Gold Layer
gold_processors = {
    "sales": GoldSales(config["gold_path"]),
    "store": GoldStore(config["gold_path"]),
    "category": GoldCategory(config["gold_path"]),
    "calendar": GoldCalendar(config["gold_path"]),
    "county": GoldCounty(config["gold_path"]),
    "vendor": GoldVendor(config["gold_path"]),
    "item": GoldItem(config["gold_path"])
}

for table in config["tables"]:
    gold_processors[table].aggregate_sales(df_silver[table])

print("Pipeline execution completed successfully!")
