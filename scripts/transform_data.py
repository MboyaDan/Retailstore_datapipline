from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, to_date, date_format
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
            StructField("sale_date", TimestampType(), False)  # Ensure timestamp type
        ])
        
        customers_schema = StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("customer_name", StringType(), False),
            StructField("email", StringType(), True),
            StructField("phone_number", StringType(), True),
            StructField("city", StringType(), True),
            StructField("signup_date", TimestampType(), True)
        ])

        # Define file paths
        sales_path = "/opt/airflow/data/extracted/sales.csv.gz"
        customers_path = "/opt/airflow/data/extracted/customers.csv.gz"
        output_path = "/opt/airflow/data/transformed/sales_customers.parquet"

        # Read extracted data with schema enforcement
        logging.info("\U0001F4E5 Reading input files...")
        df_sales = spark.read.option("header", True) \
            .option("inferSchema", True) \
            .option("mode", "DROPMALFORMED") \
            .csv(sales_path, schema=sales_schema)

        df_customers = spark.read.option("header", True) \
            .option("inferSchema", True) \
            .option("mode", "DROPMALFORMED") \
            .csv(customers_path, schema=customers_schema)
        
        logging.info("✅ Successfully read input files.")

        # Handle missing values
        df_sales = df_sales.fillna({"customer_id": -1, "quantity": 0, "price": 0.0})
        df_customers = df_customers.fillna({"email": "unknown", "phone_number": "unknown", "city": "unknown"})

        # Remove duplicate customers, keeping the latest signup_date
        window_spec = Window.partitionBy("customer_id").orderBy(col("signup_date").desc())
        df_customers = df_customers.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).drop("row_num")
        
        # Join sales with customers
        df_transformed = df_sales.join(df_customers, "customer_id", "left")

        # Convert `sale_date` to a proper date format for partitioning
        df_transformed = df_transformed.withColumn("sale_date", date_format(col("sale_date"), "yyyy-MM-dd"))

        # Select relevant columns and verify they exist before writing
        expected_columns = [
            "sale_id", "customer_id", "customer_name", "product_id", "store_id",
            "quantity", "price", "sale_date", "email", "phone_number", "city", "signup_date"
        ]

        missing_columns = [col_name for col_name in expected_columns if col_name not in df_transformed.columns]
        if missing_columns:
            logging.error(f"❌ Missing columns in transformed data: {missing_columns}")
            sys.exit(1)

        # Write the transformed data in Parquet format, partitioned by `sale_date` and `store_id`
        logging.info(f"📦 Writing transformed data to {output_path}...")
        df_transformed.write.mode("overwrite") \
            .partitionBy("sale_date", "store_id") \
            .parquet(output_path)

        logging.info("✅ Data transformation completed successfully.")

        # Stop Spark session
        spark.stop()

    except Exception as e:
        logging.error(f"❌ Error in data transformation: {str(e)}\n{traceback.format_exc()}")
        sys.exit(1)  # Exit with an error code to signal failure in Airflow

# Run the transformation function if the script is executed directly
if __name__ == "__main__":
    transform_data()
