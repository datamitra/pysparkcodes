from pyspark.sql import SparkSession
from pyspark.sql.types import *

ss = SparkSession.builder.getOrCreate()

schema = StructType([StructField('Date', StringType(), False),
                     StructField('Time', StringType(), False),
                     StructField('Size', IntegerType(), False),
                     StructField('version', StringType(), False),
                     StructField('os', StringType(), False),
                     StructField('country', StringType(), False),
                     StructField('ip_id', IntegerType(), False)])

df = ss.read.csv("/home/subbu/study/python/pythonpractice/pysparkcodes/data/Rdownload/*.csv.gz",inferSchema=True,header=True)
print(df.rdd.getNumPartitions())
print(df.select("*").limit(5).collect())
os_count = df.select("os").distinct().orderBy("os").collect()

for ros in os_count:
    print(ros['os'])

df.createOrReplaceTempView("cran_dl_log")
df.cache()

#print(ss.sql("select * from cran_dl_log  limit 5").collect())
packagecount = ss.sql("select date, count(1) dlcount from cran_dl_log group by date order by dlcount desc").collect()
#df.printSchema()

for package in packagecount:
    print("Date: %s\tDownloadCount: %i" % (package['date'], int(package['dlcount'])))

#pdf = df.groupBy("package").count().toPandas()

#print(type(pdf))