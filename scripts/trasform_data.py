from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType

# Initialize Spark Session
spark = SparkSession.builder.appName("RetailStoreETL").getOrCreate()

# Define schemas for sales and customers
sales_schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), False),
    StructField("store_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price", DoubleType(), False),
    StructField("sale_date", TimestampType(), False)
])

customers_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), False),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("signup_date", TimestampType(), True)
])

# Read extracted data with schema
df_sales = spark.read.csv("/opt/airflow/data/extracted/sales.csv", schema=sales_schema, header=True)
df_customers = spark.read.csv("/opt/airflow/data/extracted/customers.csv", schema=customers_schema, header=True)

# Data Cleaning: Handle missing values
df_sales = df_sales.fillna({"customer_id": -1, "quantity": 0, "price": 0.0})
df_customers = df_customers.fillna({"email": "unknown", "phone_number": "unknown"})

# Transformation: Join sales with customers
df_transformed = df_sales.join(df_customers, "customer_id", "left")

# Select only relevant columns
df_transformed = df_transformed.select(
    col("sale_id"),
    col("customer_id"),
    col("customer_name"),
    col("product_id"),
    col("store_id"),
    col("quantity"),
    col("price"),
    col("sale_date"),
    col("email"),
    col("phone_number"),
    col("signup_date")
)

# Partitioned write for optimization (partition by `sale_date`)
df_transformed.write.mode("overwrite").partitionBy("sale_date").parquet("/opt/airflow/data/transformed/sales_customers.parquet")

print("✅ Data transformation complete! 🚀")
