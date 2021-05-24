#python3 fit_als.py


import findspark
findspark.init()
from pyspark.sql.functions import col, explode
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

data = spark.read.csv("/tmp/orders.csv", header=True)
data = data.withColumnRenamed('product_id','item_id').withColumnRenamed('household_key','user_id')

data = data.\
    withColumn('user_id', col('user_id').cast('integer')).\
    withColumn('item_id', col('item_id').cast('integer')).\
    withColumn('quantity', col('quantity').cast('integer'))
    
data = data.withColumn('quantity', F.when(F.col("quantity") != 1, 1).otherwise(F.col("quantity")))

# Create ALS model
als = ALS(maxIter=20, rank = 20, userCol="user_id", itemCol="item_id", ratingCol="quantity", nonnegative = True, implicitPrefs = True, coldStartStrategy="drop")

t1 = time.time()

evaluator = RegressionEvaluator(metricName="rmse", labelCol="quantity", predictionCol="prediction") 

model = als.fit(data)

print('time', time.time() - t1)
print("Rank:", model._java_obj.parent().getRank())
print("MaxIter:", model._java_obj.parent().getMaxIter())
print("RegParam:", model._java_obj.parent().getRegParam())

# View the predictions
predictions = model.transform(data)
RMSE = evaluator.evaluate(predictions)
print(RMSE)

model.write().overwrite().save(path='/home/ksn38/models/als')

