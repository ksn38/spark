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

#read all csv in stream mode
raw_files = spark \
    .readStream \
    .format("json") \
    .schema(schema) \
    .options(path="input_json", header=True) \
    .load()

#set time once
load_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

#ALWAYS WRITE IN THE SAME DIRECTORY
def file_sink(df, freq):
    return df.writeStream.format("parquet") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("path","my_submit_parquet_files/p_date=" + str(load_time)) \
        .option("checkpointLocation", "checkpionts/my_parquet_checkpoint") \
        .start()

timed_files = raw_files.withColumn("p_date", F.lit("load_time"))

#stream start
stream = file_sink(timed_files,10)

#will always spark.stop() at the end

#STREAM STOPS BECAUSE THESE ALWAYS IS A SPARK.STOP() IN THE END
