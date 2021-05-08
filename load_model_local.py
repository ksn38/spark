import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('load_model').getOrCreate()
from pyspark.ml.recommendation import ALSModel


als_loaded = ALSModel.load("/home/ksn38/models/als")
# train.select('user_id').distinct().count()
nrecommendations = als_loaded.recommendForAllUsers(1)
print(nrecommendations.count())
