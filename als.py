#import findspark
#findspark.init()
#import pandas as pd
from pyspark.sql.functions import col, explode
from pyspark import SparkContext

from pyspark.sql import SparkSession

# Import the required functions
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

import time


sc = SparkContext
# sc.setCheckpointDir('checkpoint')
spark = SparkSession.builder.appName('Recommendations').getOrCreate()

data = spark.read.csv("hdfs://bigdataanalytics2-head-shdpt-v31-1-0.novalocal:8020/user/305_kozik/data/transaction_data.csv", header=True)
data = data.toDF(*[col.lower() for col in data.columns])
data = data.withColumnRenamed('product_id','item_id').withColumnRenamed('household_key','user_id')

from pyspark.sql.functions import col, trim, lower

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

# Add hyperparameters and their respective values to param_grid
param_grid = ParamGridBuilder() \
            .addGrid(als.rank, [10, 50, 100, 150]) \
            .addGrid(als.regParam, [.01, .05, .1, .15]) \
            .build()
            #             .addGrid(als.maxIter, [5, 50, 100, 200]) \

           
# Define evaluator as RMSE and print length of evaluator
evaluator = RegressionEvaluator(metricName="rmse", labelCol="quantity", predictionCol="prediction") 
print ("Num models to be tested: ", len(param_grid))

# Build cross validation using CrossValidator
cv = CrossValidator(estimator=als, estimatorParamMaps=param_grid, evaluator=evaluator, numFolds=5)

t1 = time.time()

#Fit cross validator to the 'train' dataset
model = cv.fit(train)

#Extract best model from the cv model above
best_model = model.bestModel#Fit cross validator to the 'train' dataset
model = cv.fit(train)

#Extract best model from the cv model above
best_model = model.bestModel

print(time.time() - t1)

# Print best_model
print(type(best_model))

# Complete the code below to extract the ALS model parameters
print("**Best Model**")

# # Print "Rank"
print("  Rank:", best_model._java_obj.parent().getRank())

# Print "MaxIter"
print("  MaxIter:", best_model._java_obj.parent().getMaxIter())

# Print "RegParam"
print("  RegParam:", best_model._java_obj.parent().getRegParam())

# View the predictions
test_predictions = best_model.transform(test)
RMSE = evaluator.evaluate(test_predictions)
print(RMSE)
