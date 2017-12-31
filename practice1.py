from pyspark import SparkContext

sc = SparkContext("local[4]", "practice1")

data = sc.textFile("/home/subbu/study/python/pythonpractice/pysparkcodes/data/RPackagesdownload/*.csv.gz",minPartitions=300)

packagewise = data.map(lambda x: x.split(","))\
    .map(lambda x: x[5])\
    .map(lambda x: x.replace('"', ''))\
    .map(lambda x: (x,1))\
    .reduceByKey(lambda a,b: a+b)\
    .sortBy(lambda x: x[1],ascending=False)\
#    .filter(lambda x: x[0] == 'linux-gnu')

packagecount = packagewise.collect()

for package in packagecount:
    print("Package Name: %s\tCount: %s" % (package[0], package[1]))

"""
distinct_pacakge = data.map(lambda x: x.split(","))\
    .map(lambda x: x[5])\
    .map(lambda x: x.replace('"', ''))\
    .distinct().sortBy(lambda x: x)
"""

#for package in distinct_pacakge.collect():
#    print("Package Name: %s" % package)

#print(lnx.collect())