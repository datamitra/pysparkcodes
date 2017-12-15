import json
from pyspark import SparkConf, SparkContext, SQLContext

streamdata = open("/home/subbu/PycharmProjects/data/streams/streams").readlines()
tracksdata = open("/home/subbu/PycharmProjects/data/tracks/tracks").readlines()
userdata = open("/home/subbu/PycharmProjects/data/users/users.json").readlines()
streamrec = []
tracksrec = []
userrec = []
for rec in streamdata:
    streamrec.append(json.loads(rec))
for rec in tracksdata:
    tracksrec.append(json.loads(rec))
for rec in userdata:
    userrec.append(json.loads(rec))

#pyspark code
conf = (SparkConf ()
            . setMaster("local[2]")
            . setAppName("pyspark-load-json")
            . set("spark.executor.memory", "1g"))

sqlc = SQLContext(SparkContext)
print(streamrec[0]["version"])

