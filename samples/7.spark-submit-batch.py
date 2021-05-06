from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType
import datetime

spark = SparkSession.builder.appName("gogin_spark").getOrCreate()
schema = StructType() \
    .add("date_added", StringType()) \
    .add("gspc", StringType()) \
    .add("vix", StringType()) \
    .add("tnx", StringType())

#read all csv in batch mode
raw_files = spark \
    .read \
    .format("json") \
    .schema(schema) \
    .options(path="input_json", header=True) \
    .load()

#fix timestamp
load_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
print("START BATCH LOADING. TIME = " + load_time)

#write parquet files in partitions
raw_files.withColumn("p_date", F.lit("load_time")) \
    .write \
    .mode("append") \
    .parquet("my_submit_parquet_files/p_date=" + str(load_time))

print("FINISHED BATCH LOADING. TIME = " + load_time)

spark.stop()
