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

# Define paths
silver_path = "/mnt/history/silver/"
gold_path = "/mnt/history/gold/"

# Initialize transformation classes
silver_sales = SilverSales(silver_path)
silver_store = SilverStore(silver_path)
silver_category = SilverCategory(silver_path)
silver_calendar = SilverCalendar(silver_path)
silver_county = SilverCounty(silver_path)
silver_vendor = SilverVendor(silver_path)
silver_item = SilverItem(silver_path)

# Run transformations
df_silver_sales = silver_sales.transform_data(df_bronze)
df_silver_store = silver_store.transform_data(df_bronze)
df_silver_category = silver_category.transform_data(df_bronze)
df_silver_calendar = silver_calendar.transform_data(df_bronze)
df_silver_county = silver_county.transform_data(df_bronze)
df_silver_vendor = silver_vendor.transform_data(df_bronze)
df_silver_item = silver_item.transform_data(df_bronze)




# Initialize Gold Layer processing classes
gold_sales = GoldSales(gold_path)
gold_store = GoldStore(gold_path)
gold_category = GoldCategory(gold_path)
gold_calendar = GoldCalendar(gold_path)
gold_county = GoldCounty(gold_path)
gold_vendor = GoldVendor(gold_path)
gold_item = GoldItem(gold_path)

# Run Gold Layer aggregations
df_gold_sales = gold_sales.aggregate_sales(df_silver_sales)
df_gold_store = gold_store.aggregate_store_data(df_silver_store)
df_gold_category = gold_category.aggregate_category_sales(df_silver_category)
df_gold_calendar = gold_calendar.aggregate_calendar_sales(df_silver_sales)
df_gold_county = gold_county.aggregate_county_sales(df_silver_sales)
df_gold_vendor = gold_vendor.aggregate_vendor_sales(df_silver_sales)
df_gold_item = gold_item.aggregate_item_sales(df_silver_sales)
