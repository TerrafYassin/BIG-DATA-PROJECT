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

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
conf = SparkConf().setAppName("KafkaToSpark").setMaster('local')
sc = SparkContext.getOrCreate(conf=conf)
ssc = StreamingContext(sc,20)
data = KafkaUtils.createDirectStream(ssc, topics=["SumoTop"], kafkaParams={"metadata.broker.list":"localhost:9092"})

#GET DATA ON JSON FORMAT 
dstream=data.map(lambda x: json.loads(x[1]))
dstream.pprint()

#COUNT NUMBER OF ITEMS IN EACH BATCH
dstream.count().map(lambda x:'number of items in this batch: %s' % x).pprint()

#NUMBER OF VEHICLES BY LANE AND TIME
time_lane_dstream = dstream.map(lambda place: (place['time'],place['lane']))
by_lane_time = time_lane_dstream.countByValue()
#by_lane_time.pprint()

#NUMBER OF VEHICLES BY TIME
time_dstream = dstream.map(lambda place: place['time'])
by_time = time_dstream.countByValue()
#by_time.pprint()
 

#DETECT CONGESTIONS(8 is CAPACITY MEDIUM LANES)
laneTime_dstream= dstream.map(lambda place: (place['lane'],place['time']))
by_lane_time = laneTime_dstream.countByValue().filter(lambda x : x[1]>8)
by_lane_time.pprint()

#CORDINATES OF VEHICLES IN CONGESTION 
info_dstream= dstream.map(lambda p: ((p['lane'],p['time']),(p['id'],p['x'],p['y'])))
group_info=info_dstream.groupByKey().mapValues(list).filter(lambda x : len(x[1]) > 8)
group_info.pprint()





ssc.start()
ssc.awaitTermination()
