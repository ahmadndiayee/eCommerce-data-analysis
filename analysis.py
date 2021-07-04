import os
from pyspark.sql.functions import col
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


orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv(orders)
order_items_df = spark.read.option("header", "true").option("inferSchema", "true").csv(order_items)
customers_df = spark.read.option("header", "true").option("inferSchema", "true").csv(customers)
orders_df.show()


