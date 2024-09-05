# Databricks notebook source
# MAGIC %md
# MAGIC The proposed solution will use the Medallion Architecture with three layers—Bronze, Silver, and Gold—stored in Azure Data Lake Storage (ADLS) Gen2 in the Delta Lake format. This architecture will allow us to manage data in stages, from raw ingestion to transformed and aggregated data ready for analysis, while also supporting incremental snapshot data handling to ensure data is always current.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC #### Medallion Architecture Overview
# MAGIC The Medallion Architecture organizes data into three main layers:
# MAGIC
# MAGIC 1. Bronze Layer: This layer ingests raw data from various sources and stores it in Delta format in ADLS Gen2. We will use Databricks Autoloader to handle incremental data ingestion efficiently.
# MAGIC 2. Silver Layer: The Silver Layer is responsible for cleaning, transforming, and standardizing the raw data ingested in the Bronze layer. This layer provides clean, de-duplicated, and structured data for analytical purposes.
# MAGIC 3. Gold Layer: The Gold Layer contains refined and aggregated data ready for business use cases, reporting, and analytics. This data is highly curated and organized to support dashboards and business intelligence tools like Power BI.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Bronze Layer - Incremental Data Ingestion with Autoloader
# MAGIC
# MAGIC 1. Set Up ADLS Gen2 and Azure Databricks.
# MAGIC
# MAGIC     * Create a storage account in Azure with hierarchical namespace enabled.
# MAGIC     * Create a container named "bronze" to store raw data in Delta format.
# MAGIC     * Set up an Azure Databricks workspace and configure it to securely access ADLS Gen2 using a Service Principal or Managed Identity.
# MAGIC
# MAGIC 2. Ingest Incremental Data Using Databricks Autoloader.
# MAGIC
# MAGIC     * Databricks Autoloader will be used to automatically detect new files as they arrive in the storage location and process them incrementally.
# MAGIC     * This setup allows for efficient handling of incoming data without reprocessing existing data.
# MAGIC
# MAGIC 3.  Orchestrate Data Ingestion Using Databricks Workflows:
# MAGIC     * Create a Databricks Workflow to orchestrate the data ingestion process using the Autoloader notebook.
# MAGIC     * Set up job dependencies to run data ingestion processes incrementally based on a defined schedule (e.g., daily).
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

#Below is the sample code to set up Autoloader in a Databricks notebook to ingest data incrementally into the Bronze layer:

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Define the schema for the data
schema = StructType([
    StructField("TransactionDate", StringType(), True),
    StructField("AccountNumber", IntegerType(), True),
    StructField("TransactionType", StringType(), True),
    StructField("Amount", DoubleType(), True)
])

# Set up Azure storage account details
bronze_path = "abfss://bronze@<storage_account>.dfs.core.windows.net/source1/"

# Read data incrementally using Autoloader
bronze_df = (
    spark.readStream
    .format("cloudFiles")  # Autoloader format
    .option("cloudFiles.format", "csv")  # Source data format
    .schema(schema)
    .load(bronze_path)
)

# Write ingested data to Delta format in the Bronze layer with incremental updates
bronze_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "abfss://bronze@<storage_account>.dfs.core.windows.net/source1_checkpoint/") \
    .outputMode("append") \
    .start("abfss://bronze@<storage_account>.dfs.core.windows.net/source1/")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Silver Layer - Incremental Data Transformation
# MAGIC
# MAGIC 1.  Create Databricks Notebooks for Incremental Data Transformation
# MAGIC
# MAGIC     *  Read raw data from the Bronze layer and perform data cleaning, de-duplication, normalization, and transformation.
# MAGIC     *  Implement change data capture (CDC) logic to ensure only new or changed records are processed from the Bronze layer to the Silver layer.
# MAGIC
# MAGIC 2.  Use Databricks Workflows to Automate Transformation
# MAGIC
# MAGIC     *  Create a Databricks Workflow to automate the transformation steps.
# MAGIC     *  Set up job dependencies to ensure transformation notebooks are executed after ingestion notebooks.
# MAGIC

# COMMAND ----------

# Read raw data incrementally from Bronze layer
bronze_df = spark.read.format("delta").load("abfss://bronze@<storage_account>.dfs.core.windows.net/source1/")

# Read only the changes (incremental) since the last load
incremental_bronze_df = bronze_df.where("TransactionDate > (SELECT MAX(TransactionDate) FROM silver_table)")

# Perform data cleaning and transformation
silver_df = incremental_bronze_df.dropDuplicates().filter(col("TransactionType").isNotNull())

# Merge transformed data with existing Silver layer table using upserts
silver_df.createOrReplaceTempView("silver_updates")

spark.sql("""
  MERGE INTO silver_table AS target
  USING silver_updates AS source
  ON target.AccountNumber = source.AccountNumber AND target.TransactionDate = source.TransactionDate
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")

# Write cleaned and transformed data to the Silver layer in Delta format
silver_df.write.format("delta").mode("overwrite").save("abfss://silver@<storage_account>.dfs.core.windows.net/source1/")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3: Gold Layer - Incremental Data Serving and Aggregation
# MAGIC
# MAGIC 1.  Develop Notebooks for Incremental Aggregation and Data Serving
# MAGIC
# MAGIC     *  Read transformed data from the Silver layer and perform aggregations to generate business-ready data.
# MAGIC     *  Implement logic to handle incremental data changes and append new aggregations to the existing data
# MAGIC
# MAGIC
# MAGIC 2.  Orchestrate Gold Layer Preparation Using Databricks Workflows:
# MAGIC     *  Create a Databricks Workflow to automate data preparation for the Gold layer.
# MAGIC     *  Set up dependencies to ensure this workflow runs after the Silver layer processing completes, accommodating incremental updates.
# MAGIC
# MAGIC

# COMMAND ----------

# Read data incrementally from Silver layer
silver_df = spark.read.format("delta").load("abfss://silver@<storage_account>.dfs.core.windows.net/source1/")

# Read only the new or updated data since the last load
incremental_silver_df = silver_df.where("TransactionDate > (SELECT MAX(TransactionDate) FROM gold_table)")

# Perform aggregation for dashboarding (e.g., calculating average transaction amount)
gold_df = incremental_silver_df.groupBy("AccountNumber").agg({"Amount": "avg"})

# Merge aggregated data with existing Gold layer table using upserts
gold_df.createOrReplaceTempView("gold_updates")

spark.sql("""
  MERGE INTO gold_table AS target
  USING gold_updates AS source
  ON target.AccountNumber = source.AccountNumber
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")

# Write aggregated data to the Gold layer in Delta format
gold_df.write.format("delta").mode("overwrite").save("abfss://gold@<storage_account>.dfs.core.windows.net/account_aggregates/")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 4: Data Governance Using Unity Catalog. 
# MAGIC
# MAGIC 1.  Set Up Unity Catalog: 
# MAGIC     *  Enable Unity Catalog in your Databricks workspace. 
# MAGIC     *  Create a catalog and schemas for each layer: Bronze, Silver, and Gold. 
# MAGIC     *  Register Delta tables in Unity Catalog for all layers. 
# MAGIC 2.  Manage Data Permissions: 
# MAGIC     *  Use Unity Catalog to define and manage permissions at the catalog, schema, and table levels.
# MAGIC     *  Assign permissions based on roles (e.g., data engineers, data scientists, analysts) to control access to different layers of data.
# MAGIC
# MAGIC 3.  Data Lineage and Compliance: 
# MAGIC     *  Use Unity Catalog to track data lineage, helping you understand where data comes from, how it is transformed, and where it is consumed.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5: Orchestration and Monitoring Using Databricks Workflows
# MAGIC 1.  Create a Unified Workflow:
# MAGIC     *  Combine all ingestion, transformation, and aggregation jobs into a single Databricks Workflow.
# MAGIC     *  Define task dependencies and error-handling strategies (e.g., retry mechanisms, alerts for failures).
# MAGIC
# MAGIC 2.  Monitoring and Alerting:
# MAGIC     *  Use Databricks' built-in monitoring capabilities to track workflow execution.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Conclusion
# MAGIC By implementing incremental snapshot data handling across the Bronze, Silver, and Gold layers using Databricks Autoloader and Delta Lake capabilities, the solution ensures efficient data processing, scalability, and compliance with data governance requirements. This modern approach enables real-time data availability, optimal performance, and robust data management in Azure.
# MAGIC
