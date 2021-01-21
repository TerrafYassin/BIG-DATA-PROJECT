import findspark
findspark.init(spark_home='C:\BigDataLocalSetup\Spark')
import pyspark
import json
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import datetime
import time
from pyspark.sql.types import Row
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType

conf = SparkConf().setAppName("ToSpark").setMaster('local')
sc = SparkContext.getOrCreate(conf=conf)
ssc = StreamingContext(sc,20)
data = KafkaUtils.createDirectStream(ssc, topics=["TopicS"], kafkaParams={"metadata.broker.list":"localhost:9092"})

#GET DATA ON JSON FORMAT 
dstream=data.map(lambda x: json.loads(x[1]))
dstream.pprint()

#COUNT NUMBER OF ITEMS IN EACH BATCH
dstream.count().map(lambda x:'number of items in this batch: %s' % x).pprint()


#DETECT ACCIDENT
laneTime_dstream= dstream.map(lambda x : ((x['lane'],x['speed'],x['minGap']),(x['id'],x['type'],x['x'],x['y']))).filter(lambda x : x[0][1]=='0.00'and x[0][2]=='0')

by_lane_time = laneTime_dstream.groupByKey().mapValues(set).filter(lambda x : len(x[1]) == 2)
by_lane_time.pprint()








ssc.start()
ssc.awaitTermination()
