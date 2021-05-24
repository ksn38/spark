/kafka/bin/kafka-topics.sh --create --topic tickers_json --zookeeper 10.0.0.6:2181 --partitions 1 \
--replication-factor 2 --config retention.ms=-1

/kafka/bin/kafka-console-producer.sh --topic tickers_json --broker-list 10.0.0.6:6667

/kafka/bin/kafka-console-consumer.sh --topic tickers_json --bootstrap-server 10.0.0.6:6667


/kafka/bin/kafka-topics.sh --zookeeper 10.0.0.6:2181 --delete --topic tickers_json


export SPARK_KAFKA_VERSION=0.10
/spark2.4/bin/pyspark --master local --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5

========================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType

kafka_brokers = "10.0.0.6:6667"

#функция, чтобы выводить на консоль вместо show()
def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=False) \
        .start()

#читаем без стрима
raw_orders = spark.read. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "tickers_json"). \
    option("startingOffsets", "earliest"). \
    load()

raw_orders.show()
raw_orders.show(1,False)

#прочитали до 2го оффсета
raw_orders = spark.read. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "tickers_json"). \
    option("startingOffsets", "earliest"). \
    option("endingOffsets", """{"tickers_json":{"0":2}}"""). \
    load()

raw_orders.show(100)

# прочитали в стриме ВСЁ
raw_orders = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "tickers_json"). \
    option("startingOffsets", "earliest"). \
    load()

out = console_output(raw_orders, 5)
out.stop()

# прочитали потихоньку
raw_orders = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "tickers_json"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "5"). \
    load()

out = console_output(raw_orders, 5)
out.stop()


# прочитали один раз с конца
raw_orders = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "tickers_json"). \
    option("maxOffsetsPerTrigger", "5"). \
    option("startingOffsets", "latest"). \
    load()

out = console_output(raw_orders, 5)
out.stop()


# прочитали с 10го оффсета
raw_orders = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "tickers_json"). \
    option("startingOffsets", """{"tickers_json":{"0":10}}"""). \
    option("maxOffsetsPerTrigger", "5"). \
    load()

out = console_output(raw_orders, 5)
out.stop()


##разбираем value
schema = StructType() \
    .add("date_added", StringType()) \
    .add("gspc", StringType()) \
    .add("vix", StringType()) \
    .add("tnx", StringType())


value_orders = raw_orders \
    .select(F.col("value").cast("String"), "offset")

value_orders.printSchema()

out = console_output(value_orders, 30)
out.stop()

from_json_orders = value_orders \
    .select(F.from_json(F.col("value"), schema).alias("value"), "offset")

from_json_orders.printSchema
out = console_output(from_json_orders, 30)
out.stop()

parsed_orders = from_json_orders.select("value.*", "offset")

parsed_orders.printSchema()

out = console_output(parsed_orders, 30)
out.stop()

#добавляем чекпоинт
def console_output_checkpointed(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq) \
        .option("truncate",False) \
        .option("checkpointLocation", "orders_console_checkpoint") \
        .start()

out = console_output_checkpointed(parsed_orders, 5)
out.stop()

========================================================================
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, FloatType

kafka_brokers = "10.0.0.6:6667"

#Создаем стрим, читаем из Кафки.
raw_data = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "tickers_json"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "5"). \
    load()

#Задаём структуру данных, котора содержится в топике.
schema = StructType() \
    .add("date_added", StringType()) \
    .add("gspc", FloatType()) \
    .add("vix", FloatType()) \
    .add("tnx", FloatType())
    
#Преобразуем данные в соответствии со схемой.
parsed_json = raw_data \
    .select(F.from_json(F.col("value").cast("String"), schema).alias("value"), "offset") \
    .select("value.*", "offset")
    

#Задаём метод для вывода стрима на консоль.
def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=False) \
        .start()

#Проверяем.
out = console_output(parsed_json, 10)
out.stop()

#MEMORY SINK
#Метод для записи стрима в оперативную память. Обязательный параметр `queryName` задаёт название таблицы в памяти Спарка, в которой будут храниться прочитанные даные. Эта таблица существует только в рамках сессии и есть только в памяти драйвера. Из-за этого этот синк не параллелится и используется только для тестов. 

def memory_sink(df, freq):
    return df.writeStream.format("memory") \
        .queryName("my_memory_sink_table") \
        .trigger(processingTime='%s seconds' % freq ) \
        .start()

stream = memory_sink(parsed_json, 5)

#Проверим что лежит в таблице `my_memory_sink_table`.
spark.sql("select * from my_memory_sink_table").show()

#FILE SINK (only with checkpoint)
#Метод для записи стрима в файлы. Обязательная опция `path` указывает путь на HDFS, по которому будут сохраняться файлы. Обязательная опция `checkpointLocation` указывает на папку с чекпойнтами (аналогично как на занятии 3).

def file_sink(df, freq):
    return df.writeStream.format("parquet") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("path","my_parquet_sink") \
        .option("checkpointLocation", "json_file_checkpoint") \
        .start()

stream = file_sink(parsed_json, 5)
stream.stop()

#Так как на каждый файл в HDFS создаётся запись в name-ноде о месте расположения файла и его реплик, то большое количество файлов маленького размера могут тормозить работу с HDFS. Оптимальным будет писать файлы в директорию с партиционированием по дате (.option("path","my_parquet_sink/p_date=20201215")), а на следующий день собирать все файлы из директории предыдущего дня и склеивать их в несколько файлов большого размера. Это возможно, так как при таком подходе в директорию прошлых дней не будут добавлятсья файлы. В задании 2 написана функция для сжатия файлов.

#KAFKA SINK
#В первом терминале pyspark определяем метод для записи в созданный топик. Здесь преобразуем наши данные из структуры в строку. В отличии от настройки чтения из Кафки, топик указывается не в опции `subscribe`, а в опции `topic`. 

def kafka_sink(df, freq):
    return df.selectExpr("CAST(null AS STRING) as key", "CAST(struct(*) AS STRING) as value") \
        .writeStream \
        .format("kafka") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("topic", "tickers_json") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("checkpointLocation", "json_kafka_checkpoint") \
        .start()
        
stream = kafka_sink(parsed_json, 5)
stream.stop()

#Так как мы делали преобразование структуры в строку, то информация о названгии ключей и типах данных потерялась. Для лучшей читаемости данных последующими консьюмрами, лучше преобразовывать данные в JSON-строку.
def kafka_sink_json(df, freq):
    return df.selectExpr("CAST(null AS STRING) as key", "CAST(to_json(struct(*)) AS STRING) as value") \
        .writeStream \
        .format("kafka") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("topic", "tickers_json") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("checkpointLocation", "json_kafka_checkpoint") \
        .start()
        
stream = kafka_sink(parsed_json, 5)
stream.stop()

#FOREACH BATCH SINK
#Добавим к датасету `parsed_iris` метку времени, когда обрабатывался микробатч.
extended_json = parsed_json.withColumn("my_current_time", F.current_timestamp())

#В синке `foreach batch` вместо формата указывается функция, которая будет получать микробатч и его порядковый номер. Внутри функции с микробатчем можно работать как со статическим датафреймом.
def foreach_batch_sink(df, freq):
    return  df \
        .writeStream \
        .foreachBatch(foreach_batch_function) \
        .trigger(processingTime='%s seconds' % freq ) \
        .start()
        
#Функция, которая будет обрабатывать микробатч:
def foreach_batch_function(df, epoch_id):
    print("starting epoch " + str(epoch_id) )
    print("average values for batch:")
    df.groupBy("vix").avg().show()
    print("finishing epoch " + str(epoch_id))
    
stream = foreach_batch_sink(extended_json, 5)
