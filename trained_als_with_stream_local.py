#/home/kafka/kafka/bin/kafka-topics.sh --create --topic als_kafka2 --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --config retention.ms=-1
#/home/kafka/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic als_kafka2

#/home/kafka/kafka/bin/kafka-console-producer.sh --topic als_kafka2 --broker-list localhost:9092
#/home/kafka/kafka/bin/kafka-console-consumer.sh --topic als_kafka2 --bootstrap-server localhost:9092

#export SPARK_KAFKA_VERSION=0.10
#opt/spark/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 1024m --driver-cores 1 --master local[1]


from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, FloatType
from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALSModel

spark = SparkSession.builder.appName("ksn38_spark").getOrCreate()

kafka_brokers = "localhost:9092"

#читаем кафку по одной записи, но можем и по 1000 за раз
als_data = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "als_kafka2"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "1"). \
    load()

schema = StructType().\
    add('user_id', IntegerType()).\
    add('item_id', IntegerType()).\
    add('quantity', IntegerType())

value_als = als_data.select(F.from_json(F.col("value").cast("String"), schema).alias("value"), "offset")

als_flat = value_als.select(F.col("value.*"), "offset")

def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=True) \
        .start()

s = console_output(als_flat, 3)
s.stop()

###############
#подгружаем ML из HDFS
#pipeline_model = PipelineModel.load("/home/ksn38/models/als")
als_loaded = ALSModel.load("/home/ksn38/models/als")

##########
#вся логика в этом foreachBatch
def writer_logic(df, epoch_id):
    df.persist()
    print("---------I've got new batch--------")
    print("This is what I've got from Kafka:")
    df.show()
    predict = als_loaded.transform(df)
    print("Here is what I've got after model transformation:")
    predict.show()
    #обновляем исторический агрегат в касандре
    df.unpersist()

#связываем источник Кафки и foreachBatch функцию
stream = als_flat \
    .writeStream \
    .trigger(processingTime='20 seconds') \
    .foreachBatch(writer_logic) \
    .option("checkpointLocation", "checkpoints/sales_unknown_checkpoint")

#поехали
s = stream.start()
s.stop()
