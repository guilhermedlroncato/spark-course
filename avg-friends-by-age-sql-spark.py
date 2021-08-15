from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql import Row

spark = SparkSession.builder.appName("AvgByFrinedsSparkSQL").getOrCreate()

people = spark.read.option("header", "true")\
                   .option("inferSchema", "true")\
                   .csv("./data/fakefriends-header.csv")
    
people.printSchema()

# calculando média de amigos por idade, ordenando por idade e incluindo um alias na coluna com a avg
people.groupBy('age').agg(func.round(func.avg('friends'),2).alias('avg_friends')).sort('age').show()

spark.stop()

