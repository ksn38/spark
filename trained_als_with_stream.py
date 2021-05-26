#/home/kafka/kafka/bin/kafka-topics.sh --create --topic als_kafka2 --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --config retention.ms=-1
#/home/kafka/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic als_kafka2

#kafka-configs.sh --zookeeper localhost:2181 --alter --entity-type topics --add-config retention.ms=1000 --entity-name text_topic
#kafka-configs.sh --zookeeper localhost:2181 --entity-type topics --describe --entity-name text_topic

#/home/kafka/kafka/bin/kafka-console-producer.sh --topic als_kafka2 --broker-list localhost:9092
#/home/kafka/kafka/bin/kafka-console-consumer.sh --topic als_kafka2 --bootstrap-server localhost:9092

#export SPARK_KAFKA_VERSION=0.10
#/opt/spark/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5
#/opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 trained_als_with_stream.py

#{"user_id":1598}
#{"user_id":2375}
#{"user_id":3000}


from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, FloatType
from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALSModel


spark = SparkSession.builder.appName("ksn38_spark").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

kafka_brokers = "localhost:9092"

#read kafka by 1 record
als_data = spark.readStream.\
    format("kafka").\
    option("kafka.bootstrap.servers", kafka_brokers).\
    option("subscribe", "als_kafka2").\
    option("startingOffsets", "earliest").\
    option("maxOffsetsPerTrigger", "1").\
    load()

schema = StructType().\
    add('user_id', IntegerType())

value_als = als_data.select(F.from_json(F.col("value").cast("String"), schema).alias("value"), "offset")

als_flat = value_als.select(F.col("value.*"), "offset")

'''def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=True) \
        .start()

s = console_output(als_flat, 3)
s.stop()'''

#get top5
data = spark.read.csv("/tmp/orders.csv", header=True)
data = data.withColumnRenamed('product_id','item_id').withColumnRenamed('household_key','user_id')

data = data.\
    withColumn('user_id', F.col('user_id').cast('integer')).\
    withColumn('item_id', F.col('item_id').cast('integer')).\
    withColumn('quantity', F.col('quantity').cast('integer'))
    
data = data.withColumn('quantity', F.when(F.col("quantity") != 1, 1).otherwise(F.col("quantity")))

top5 = data.groupBy('item_id').agg(F.count('item_id').alias('rating'))\
    .orderBy('rating', ascending=False).take(5)

#load als
als_loaded = ALSModel.load("/home/ksn38/models/als")

#all logick in this foreachBatch
def writer_logic(df, epoch_id):
    df.persist()
    print("---------I've got new batch--------")
    print("This is what I've got from Kafka:")
    df.show()
    predict = als_loaded.recommendForUserSubset(df, 5)
    print(predict.select("recommendations").count())
    print("Here is what I've got after model transformation:")
    if predict.select("recommendations").count() == 1:
        predict = predict\
            .withColumn("rec_exp", F.explode("recommendations"))\
            .select('user_id', F.col("rec_exp.item_id"), F.col("rec_exp.rating"))
        predict.show()
    else:
        print(top5)
    df.unpersist()

#bind source of kafka with foreachBatch in function
stream = als_flat \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .foreachBatch(writer_logic) \
    #.option("checkpointLocation", "checkpoints/sales_unknown_checkpoint") \

s = stream.start()
s.awaitTermination()
#s.stop()



