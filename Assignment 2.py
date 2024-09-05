# Databricks notebook source
from pyspark.sql.functions import col, when, sum as spark_sum, to_date, row_number
from pyspark.sql.window import Window

# COMMAND ----------

#read sample data file
df = spark.read.format("csv").option("header", True).option("inferSchema", True).load("/FileStore/tables/assignment.csv")
display(df)

# COMMAND ----------

# Converting TransactionDate to date type
df = df.withColumn("TransactionDate", to_date(col("TransactionDate"), "yyyyMMdd"))
display(df)

# COMMAND ----------

# Adding column to handle Debit and Credit amount balance
df = df.withColumn("Amount", when(col("TransactionType").contains("Credit"), col("Amount"))
                   .otherwise(-col("Amount")))
display(df)

# COMMAND ----------

# Window function to sort transactions for each account
window_spec = Window.partitionBy("AccountNumber").orderBy("TransactionDate")

# Assigning row number to each transaction
df = df.withColumn("RowNum", row_number().over(window_spec))

# Create a DataFrame with running totals using self-join
result_df1 = df.alias("df1").join(
    df.alias("df2"),
    (col("df1.AccountNumber") == col("df2.AccountNumber")) & (col("df1.RowNum") >= col("df2.RowNum")),
    "left"
).groupBy(
    "df1.TransactionDate", "df1.AccountNumber", "df1.TransactionType", "df1.Amount"
).agg(
    spark_sum("df2.Amount").alias("CurrentBalance")
).orderBy(
    "AccountNumber", "TransactionDate"
)

# COMMAND ----------

display(result_df1)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Using SPARK SQL

# COMMAND ----------

from pyspark.sql.functions import expr
#read sample data file
df = spark.read.format("csv").option("header", True).option("inferSchema", True).load("/FileStore/tables/assignment.csv")
#display(df)
# Converting TransactionDate to date type
df = df.withColumn("TransactionDate", to_date(col("TransactionDate"), "yyyyMMdd"))
#display(df)

df.createOrReplaceTempView("transactions")

# SQL query to calculate running balance
result_df2 = spark.sql("""
    SELECT 
    TransactionDate,
    AccountNumber,
    TransactionType,
    Amount,
    SUM(
        CASE 
            WHEN TransactionType LIKE '%Credit%' THEN Amount 
            ELSE -Amount 
        END
    ) OVER (PARTITION BY AccountNumber ORDER BY TransactionDate) AS CurrentBalance
FROM transactions
ORDER BY AccountNumber, TransactionDate;

""")


display(result_df2)
