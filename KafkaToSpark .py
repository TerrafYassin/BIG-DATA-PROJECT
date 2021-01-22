import findspark
findspark.init(spark_home='D:\spark')
import pyspark

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

conf = SparkConf().setAppName("KafkaToSpark").setMaster('local')
sc = SparkContext.getOrCreate(conf=conf)

ssc = StreamingContext(sc,10)
#data = KafkaUtils.createStream(ssc, 'localhost:9092',{'topics':'my_topic'},{'groupId':'test-consumer-groupe'})
data = KafkaUtils.createDirectStream(ssc, topics=["quickstart-events"], kafkaParams={"metadata.broker.list":"localhost:9092"})
data.pprint()

ssc.start()
ssc.awaitTermination()
