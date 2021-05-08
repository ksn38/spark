/kafka/bin/kafka-topics.sh --create --topic titanic --zookeeper 10.0.0.6:2181 --partitions 1 \
--replication-factor 2 --config retention.ms=-1

/kafka/bin/kafka-console-producer.sh --topic titanic --broker-list 10.0.0.6:6667

/kafka/bin/kafka-console-consumer.sh --topic titanic --bootstrap-server 10.0.0.6:6667


export SPARK_KAFKA_VERSION=0.10
/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[1]


from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, FloatType
from pyspark.sql import functions as F
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoderEstimator, VectorAssembler, CountVectorizer, StringIndexer, IndexToString
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.appName("ksn38_spark").getOrCreate()

kafka_brokers = "10.0.0.6:6667"

#читаем кафку по одной записи, но можем и по 1000 за раз
als_data = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "titanic"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "1"). \
    load()

schema = StructType(). \
    add('user_id', IntegerType()).\
    add('item_id', IntegerType().\
    add('quantity', IntegerType())

value_als = als_data.select(F.from_json(F.col("value").cast("String"), schema).alias("value"), "offset")

als_flat = value_als.select(F.col("value.*"), "offset")

def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=True) \
        .start()

s = console_output(passengers_flat, 10)
s.stop()

###############
#подгружаем ML из HDFS
pipeline_model = PipelineModel.load("/home/ksn38/models/als")

##########
#вся логика в этом foreachBatch
def writer_logic(df, epoch_id):
    df.persist()
    print("---------I've got new batch--------")
    print("This is what I've got from Kafka:")
    df.show()
    predict = pipeline_model.transform(df)
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
