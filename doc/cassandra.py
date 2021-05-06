#/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[1]

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("gogin_spark").getOrCreate()

#функция, чтобы просматривать план запроса
def explain(self, extended=True):
    if extended:
        print(self._jdf.queryExecution().toString())
    else:
        print(self._jdf.queryExecution().simpleString())

#читаем статическую таблицу из касандры
cass_animals_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="animals", keyspace="ksn38") \
    .load()

cass_animals_df.printSchema()
cass_animals_df.show()

cass_animals_df.write.parquet("my_parquet_from_cassandra")

#теперь будем записывать данные в касандру
#для начала готовим синтетическую строчку
cow_df = spark.sql("""select 11 as id, "Cow" as name, "Big" as size """)
cow_df.show()
#пишем
cow_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="animals", keyspace="ksn38") \
    .mode("append") \
    .save()

#айди такой же, имя другое
bull_df = spark.sql("""select 11 as id, "Bull" as name, "Big" as size """)
bull_df.show()

bull_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="animals", keyspace="ksn38") \
    .mode("append") \
    .save()

#выдаст предупреждение и попросит дополнительный параметр из за overwrite
bull_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="animals", keyspace="ksn38") \
    .mode("overwrite") \
    .save()


#теперь читаем большой большой датасет по ключу
cass_big_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="users_many", keyspace="ksn38") \
    .load()

cass_big_df.show()
#выполняем запрос попадая в ключ
cass_big_df.filter(F.col("user_id")=="4164237664").show()
#выполняем запрос не попадая в ключ
cass_big_df.filter(F.col("gender")=="1").show() #быстро из за предела в 20
cass_big_df.filter(F.col("gender")=="1").count() #медленно, т.к. нужно читать всю таблицу

#Наблюдаем на pushedFilter в PhysicalPlan
explain(cass_big_df.filter(F.col("user_id")=="10"))

explain(cass_big_df.filter(F.col("gender")=="10"))

#between не передается в pushdown
between_select = cass_big_df.filter(F.col("user_id").between(4164237664, 4164237664+1) )

#проверяем, что user id не попал в pushedFilter
explain(between_select)
between_select.show() #медленно

#in передается в pushdown
in_select = cass_big_df.filter(F.col("user_id").isin(4164237664, 4164237664+1) )

#проверяем, что user id попал в pushedFilter
explain(in_select)
in_select.show() #быстро
