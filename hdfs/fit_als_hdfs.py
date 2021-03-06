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
    withColumn('item_id', col('item_id').cast('integer')).\
    withColumn('quantity', col('quantity').cast('integer'))
    
data = data.withColumn('quantity', F.when(F.col("quantity") != 1, 1).otherwise(F.col("quantity")))

# Create test and train set
train = data.select('*').where(col('week_no') < 95)
test = data.select('*').where(col('week_no') >= 95)

# Create ALS model
als = ALS(maxIter=20, rank = 20, userCol="user_id", itemCol="item_id", ratingCol="quantity", nonnegative = True, implicitPrefs = True, coldStartStrategy="drop")

t1 = time.time()

evaluator = RegressionEvaluator(metricName="rmse", labelCol="quantity", predictionCol="prediction") 

model = als.fit(train)

# View the predictions
test_predictions = model.transform(test)
RMSE = evaluator.evaluate(test_predictions)
print(RMSE)

model.write().overwrite().save(path='hdfs://bigdataanalytics2-head-shdpt-v31-1-0.novalocal:8020/user/305_kozik/models/als')
