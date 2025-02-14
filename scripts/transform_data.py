from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
)
from pyspark.sql.window import Window
import logging
import sys
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO)

def transform_data():
    try:
        # Initialize Spark Session
        spark = SparkSession.builder.appName("RetailStoreETL").getOrCreate()
        
        # Define schemas
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
            StructField("city", StringType(), True),  # ✅ Added missing column
            StructField("signup_date", TimestampType(), True)
        ])

        # Define file paths
        sales_path = "/opt/airflow/data/extracted/sales.csv.gz"
        customers_path = "/opt/airflow/data/extracted/customers.csv.gz"
        output_path = "/opt/airflow/data/transformed/sales_customers.parquet"

        # Read extracted data
        logging.info("📥 Reading input files...")
        df_sales = spark.read.csv(sales_path, schema=sales_schema, header=True)
        df_customers = spark.read.csv(customers_path, schema=customers_schema, header=True)
        logging.info("✅ Successfully read input files.")

        # Handle missing values
        df_sales = df_sales.fillna({"customer_id": -1, "quantity": 0, "price": 0.0})
        df_customers = df_customers.fillna({"email": "unknown", "phone_number": "unknown", "city": "unknown"})

        # Remove duplicate customers, keeping the latest signup_date
        window_spec = Window.partitionBy("customer_id").orderBy(col("signup_date").desc())
        df_customers = df_customers.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).drop("row_num")
        
        # Join sales with customers
        df_transformed = df_sales.join(df_customers, "customer_id", "left")

        # Select relevant columns
        df_transformed = df_transformed.select(
            col("sale_id"), col("customer_id"), col("customer_name"),
            col("product_id"), col("store_id"), col("quantity"), col("price"),
            col("sale_date"), col("email"), col("phone_number"), col("city"), col("signup_date")
        )

        # Write the transformed data in Parquet format, partitioned by `sale_date` and `store_id`
        logging.info("💾 Writing transformed data to Parquet...")
        df_transformed.write.mode("overwrite").partitionBy("sale_date", "store_id").parquet(output_path)
        logging.info("✅ Data transformation complete! 🚀")

        # Stop Spark session
        spark.stop()

    except Exception as e:
        logging.error(f"❌ Error in data transformation: {str(e)}\n{traceback.format_exc()}")
        sys.exit(1)  # Exit with an error code to signal failure in Airflow

# Run the transformation function if the script is executed directly
if __name__ == "__main__":
    transform_data()
