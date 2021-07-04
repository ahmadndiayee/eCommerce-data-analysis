import os
from pyspark.sql.functions import count, sum, col
from pyspark.sql import SparkSession
import configparser

CONF_FILE = 'config.ini'

config = configparser.ConfigParser()
config.read(os.path.expanduser("~/.aws/credentials"))
access_id = config.get("e-commerce", "aws_access_key_id")
access_key = config.get("e-commerce", "aws_secret_access_key")

spark = SparkSession.builder \
    .appName("GLUE_JOBS") \
    .master("local[*]") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.hadoop.fs.s3a.access.key", access_id) \
    .config("spark.hadoop.fs.s3a.secret.key", access_key) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

config.read(CONF_FILE)
orders = config.get("paths", 'orders')
order_items = config.get("paths", 'order_items')
customers = config.get("paths", 'customers')
output = config.get("paths", 'output')

orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv(orders)
order_items_df = spark.read.option("header", "true").option("inferSchema", "true").csv(order_items)
customers_df = spark.read.option("header", "true").option("inferSchema", "true").csv(customers)


# orders_df.printSchema()
# order_items_df.printSchema()
# customers_df.printSchema()

def join_dataset():
    df = customers_df \
        .join(orders_df, customers_df.customer_id == orders_df.customer_id) \
        .join(order_items_df, orders_df.order_id == order_items_df.order_id) \
        .select(orders_df.customer_id, col("customer_city"), col("customer_state"), orders_df.order_id, col("price"))
    return df


def get_top_customers():
    df = join_dataset() \
        .groupBy(col("customer_id")) \
        .agg(count("order_id").alias("number_of_orders"), sum("price").alias("total_price")) \
        .orderBy(col("total_price"), ascending=False)

    return df


def write_result_to_s3(df, format_output):
    df.write.format(format_output).save(output)
