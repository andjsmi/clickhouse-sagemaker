import pyspark
import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.appName('ClickHouses').getOrCreate()

# set spark config. note user, password, database and host
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
