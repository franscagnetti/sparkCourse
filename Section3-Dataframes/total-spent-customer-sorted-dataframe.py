from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalSpentCustomerSorted").master("local[*]").getOrCreate()

schema = StructType([ \
                     StructField("clientID", IntegerType(), True), \
                     StructField("itemID", IntegerType(), True), \
                     StructField("moneySpent", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("file:///SparkCourse/customer-orders.csv")
df.printSchema()

clientIDMoneySpent = df.select("clientID", "moneySpent")
clientIDMoneySpent.show()

totalSpentByCustomerSorted = clientIDMoneySpent.groupBy("clientID").agg(func.round(func.sum("moneySpent"), 2).alias("totalMoneySpent")).sort("totalMoneySpent")
totalSpentByCustomerSorted.show(totalSpentByCustomerSorted.count())
    
spark.stop()
