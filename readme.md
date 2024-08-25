# Iowa Liquor Sales Data Pipeline

## Overview
This project implements a **Medallion Architecture (Bronze, Silver, Gold)** ETL pipeline in **Databricks** using **PySpark**.

### **Architecture**
1. **Bronze Layer**: Raw JSON data ingestion.
2. **Silver Layer**: Data transformation, cleaning, type casting.
3. **Gold Layer**: Aggregation and final reporting.

---

## Folder Structure
```
iowa_liquor_pipeline/ 
│── src/ 
│ ├── bronze/ # Raw data ingestion 
│ ├── silver/ # Data cleaning & transformation 
│ ├── gold/ # Aggregations & business metrics 
│── config/ 
│ ├── config.yaml # Configuration file 
│── scripts/ 
│ ├── run_pipeline.py # Main execution script
```


---

## Setup & Execution

### **1. Upload to Databricks**
1. Open Databricks.
2. Go to **Workspace** → **Upload Files**.
3. Upload the `iowa_liquor_pipeline/` directory.

### **2. Run Pipeline in Databricks Notebook**
Execute the following command inside a **Databricks Notebook**:
```python
%run /Workspace/iowa_liquor_pipeline/scripts/run_pipeline
```
### **3. Automate via Databricks Jobs**

1.  Go to **Workflows** → **Jobs**.
2.  Click **Create Job**.
3.  Set **Task Type** = **Python Script**.
4.  Select `/Workspace/iowa_liquor_pipeline/scripts/run_pipeline.py`.
5.  Click **Create** to automate the pipeline.