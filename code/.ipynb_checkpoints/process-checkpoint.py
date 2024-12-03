import pyspark
import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

packages = [
    "com.clickhouse.spark:clickhouse-spark-runtime-3.3_2.12:0.8.0",
    "com.clickhouse:clickhouse-client:0.6.3",
    "com.clickhouse:clickhouse-http-client:0.6.3",
    "org.apache.httpcomponents.client5:httpclient5:5.2.1"

]

spark = SparkSession.builder.appName('ClickingHouses').getOrCreate()

spark.conf.set("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
spark.conf.set("spark.sql.catalog.clickhouse.host", "db.clickhouse.local")
spark.conf.set("spark.sql.catalog.clickhouse.protocol", "http")
spark.conf.set("spark.sql.catalog.clickhouse.http_port", "8123")
spark.conf.set("spark.sql.catalog.clickhouse.user", "default")
spark.conf.set("spark.sql.catalog.clickhouse.password", "")
spark.conf.set("spark.sql.catalog.clickhouse.database", "default")
spark.conf.set("spark.clickhouse.write.format", "json")

df = spark.sql("SHOW DATABASES")
df.show()

df = spark.sql("select fare_amount, pickup_date, pickup_datetime, dropoff_date, dropoff_datetime, pickup_longitude, pickup_latitude, dropoff_latitude, dropoff_longitude, passenger_count, trip_distance from clickhouse.default.trips")
df.show(5)

df.write.format('parquet').mode('Overwrite').save('s3://andjsmi-data-testing/clickout/')
