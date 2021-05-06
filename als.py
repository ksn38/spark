from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import functions as F
import time


sc = SparkContext
# sc.setCheckpointDir('checkpoint')
spark = SparkSession.builder.appName('Recommendations').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data = spark.read.csv("hdfs://bigdataanalytics2-head-shdpt-v31-1-0.novalocal:8020/user/305_kozik/data/transaction_data.csv", header=True)
data = data.toDF(*[col.lower() for col in data.columns])
data = data.withColumnRenamed('product_id','item_id').withColumnRenamed('household_key','user_id')

from pyspark.sql.functions import col, lower, explode

data = data.\
    withColumn('user_id', col('user_id').cast('integer')).\
    withColumn('basket_id', col('basket_id').cast('integer')).\
    withColumn('day', col('day').cast('integer')).\
    withColumn('item_id', col('item_id').cast('integer')).\
    withColumn('quantity', col('quantity').cast('integer')).\
    withColumn('sales_value', col('sales_value').cast('float')).\
    withColumn('store_id', col('store_id').cast('integer')).\
    withColumn('retail_disc', col('retail_disc').cast('float')).\
    withColumn('week_no', col('week_no').cast('integer')).\
    withColumn('coupon_disc', col('coupon_disc').cast('integer')).\
    withColumn('coupon_match_disc', col('coupon_match_disc').cast('float')).\
    drop('trans_time')
    
data = data.withColumn('quantity', F.when(F.col("quantity") != 1, 1).otherwise(F.col("quantity")))

# Create test and train set
train = data.select('*').where(col('week_no') < 95)
test = data.select('*').where(col('week_no') >= 95)

# Create ALS model
als = ALS(maxIter=20, rank = 20, userCol="user_id", itemCol="item_id", ratingCol="quantity", nonnegative = True, implicitPrefs = True, coldStartStrategy="drop")

t1 = time.time()

evaluator = RegressionEvaluator(metricName="rmse", labelCol="quantity", predictionCol="prediction") 

model = als.fit(train)

print('time', time.time() - t1)
print("Rank:", model._java_obj.parent().getRank())
print("MaxIter:", model._java_obj.parent().getMaxIter())
print("RegParam:", model._java_obj.parent().getRegParam())

# View the predictions
test_predictions = model.transform(test)
RMSE = evaluator.evaluate(test_predictions)
print(RMSE)

'''nrecommendations = model.recommendForAllUsers(10)
nrecommendations.limit(10).show()

nrecommendations = nrecommendations\
    .withColumn("rec_exp", explode("recommendations"))\
    .select('user_id', col("rec_exp.item_id"), col("rec_exp.rating"))
nrecommendations.limit(10).show()'''
