import findspark
findspark.init()
import pandas as pd
from pyspark.sql.functions import col, explode
from pyspark import SparkContext

from pyspark.sql import SparkSession

# Import the required functions
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql import functions as F

import time


sc = SparkContext
# sc.setCheckpointDir('checkpoint')
spark = SparkSession.builder.appName('Recommendations').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data = spark.read.csv("/home/ksn38/data/transaction_data.csv", header=True)
data = data.toDF(*[col.lower() for col in data.columns])
data = data.withColumnRenamed('product_id','item_id').withColumnRenamed('household_key','user_id')

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

# Count the total number of data in the dataset
numerator = data.select("quantity").count()
print('numerator', numerator)

# Count the number of distinct userIds and distinct movieIds
num_users = data.select("user_id").distinct().count()
num_items = data.select("item_id").distinct().count()
print('num_users', num_users)
print('num_items', num_items)

# Set the denominator equal to the number of users multiplied by the number of movies
denominator = num_users * num_items
print('denominator', denominator)

# Divide the numerator by the denominator
sparsity = (1.0 - (numerator *1.0)/denominator)*100
print("The data dataframe is ", "%.2f" % sparsity + "% empty.")

# Create test and train set
(train, test) = data.randomSplit([0.8, 0.2], seed = 1234)

# Create ALS model
als = ALS(userCol="user_id", itemCol="item_id", ratingCol="quantity", nonnegative = True, implicitPrefs = False, coldStartStrategy="drop")

t1 = time.time()

evaluator = RegressionEvaluator(metricName="rmse", labelCol="quantity", predictionCol="prediction") 

model = als.fit(train)

print('time', time.time() - t1)

# Complete the code below to extract the ALS model parameters
print("Model")

# # Print "Rank"
print("  Rank:", model._java_obj.parent().getRank())

# Print "MaxIter"
print("  MaxIter:", model._java_obj.parent().getMaxIter())

# Print "RegParam"
print("  RegParam:", model._java_obj.parent().getRegParam())

# View the predictions
test_predictions = model.transform(test)
RMSE = evaluator.evaluate(test_predictions)
print(RMSE)

nrecommendations = model.recommendForAllUsers(10)
nrecommendations.limit(10).show()

nrecommendations = nrecommendations\
    .withColumn("rec_exp", explode("recommendations"))\
    .select('user_id', col("rec_exp.item_id"), col("rec_exp.rating"))
nrecommendations.limit(10).show()
