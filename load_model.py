from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('load_model').getOrCreate()
from pyspark.ml.recommendation import ALSModel


als_loaded = ALSModel.load("hdfs://bigdataanalytics2-head-shdpt-v31-1-0.novalocal:8020/user/305_kozik/models/als")
# train.select('user_id').distinct().count()
nrecommendations = als_loaded.recommendForAllUsers(1)
print(nrecommendations.count())
