from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext("local[2]", "practice1")

data = sc.textFile("/home/subbu/study/python/pythonpractice/pysparkcodes/2017-12-26.csv")

packagewise = data.map(lambda x: x.split(","))\
    .map(lambda x: x[5])\
    .map(lambda x: x.replace('"', ''))\
    .map(lambda x: (x,1))\
    .reduceByKey(lambda a,b: a+b)\
    .sortBy(lambda x: x[1],ascending=False)\
    .filter(lambda x: x[0] == 'linux-gnu')

packagecount = packagewise.collect()

distinct_pacakge = data.map(lambda x: x.split(","))\
    .map(lambda x: x[5])\
    .map(lambda x: x.replace('"', ''))\
    .distinct().sortBy(lambda x: x)

for package in packagecount:
    print("Package Name: %s\tCount: %s" % (package[0], package[1]))

for package in distinct_pacakge.collect():
    print("Package Name: %s" % package)

#print(lnx.collect())