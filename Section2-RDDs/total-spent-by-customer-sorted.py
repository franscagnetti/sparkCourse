from pyspark import SparkConf, SparkContext
import collections


def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    moneySpent = float(fields[2])
    return (customerID, moneySpent)


conf = SparkConf().setMaster("local").setAppName("totalSpentByCustomerSorted")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
rdd = lines.map(parseLine)
totalsByClientID = rdd.reduceByKey(lambda x, y: x + y)

totalsByClientIDSorted = totalsByClientID.map(lambda x: (x[1], x[0])).sortByKey()

results = totalsByClientIDSorted.collect()
for result in results:
    print(result)