from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas

ss = SparkSession.builder.getOrCreate()

schema = StructType([StructField('Date', StringType(), False),
                     StructField('Time', StringType(), False),
                     StructField('Size', IntegerType(), False),
                     StructField('r_version', StringType(), False),
                     StructField('r_arch', StringType(), False),
                     StructField('r_os', StringType(), False),
                     StructField('package', StringType(), False),
                     StructField('version', StringType(), False),
                     StructField('country', StringType(), False),
                     StructField('id', IntegerType(), False)])

df = ss.read.csv("/home/subbu/study/python/pythonpractice/pysparkcodes/data/*.csv.gz",schema=schema,header=True)
print(df.rdd.getNumPartitions())
print(df.select("*").limit(5).collect())
os_count = df.select("r_os").distinct().orderBy("r_os").collect()

for ros in os_count:
    print(ros['r_os'])

df.createOrReplaceTempView("cran_dl_log")
df.cache()

#print(ss.sql("select * from cran_dl_log  limit 5").collect())
packagecount = ss.sql("select package, count(1) packagecount from cran_dl_log group by package order by packagecount desc").collect()
#df.printSchema()

for package in packagecount:
    print("Package Name: %s\tDownloadCount: %i" % (package['package'], int(package['packagecount'])))

#pdf = df.groupBy("package").count().toPandas()

#print(type(pdf))