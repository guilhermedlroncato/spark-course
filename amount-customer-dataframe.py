from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("AmountCustomerDataFrame").getOrCreate()

schema = StructType([ \
                     StructField("customerID", IntegerType(), True), \
                     StructField("productID", IntegerType(), True), \
                     StructField("dollarAmount", FloatType(), True)
                    ])

# // Read the file as dataframe
df = spark.read.schema(schema)\
               .csv("./data/customer-orders.csv")
df.printSchema()

# Agrupo o total de vendas por customer
total_customer_spent = df.groupBy('customerID')\
                         .agg(func.round(func.sum('dollarAmount'), 2).alias('dollarAmount'))\
                         .sort('dollarAmount')

total_customer_spent.show(total_customer_spent.count())

spark.stop()