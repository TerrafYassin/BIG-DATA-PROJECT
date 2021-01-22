
import findspark
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import StringIndexer
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
import pyspark.sql.functions as func
from pyspark.ml.classification import LinearSVC
from pyspark.ml.classification import NaiveBayes
from pyspark.sql.functions import col
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import RandomForestClassifier

#connexion to mongoDB
sparkt = SparkSession.builder.appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/myalldata.vehicule") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/myalldata.accuracies") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.2.0') \
    .getOrCreate()


#read data from mongo
df = sparkt.read.format("com.mongodb.spark.sql.DefaultSource").load()
df.printSchema()


#display data
df.show(truncate=False)

#convert columns from string to Float
df = df.withColumn("speed", df["speed"].cast(FloatType()))
df = df.withColumn("time", df["time"].cast(FloatType()))
df = df.withColumn("id", df["id"].cast(IntegerType()))


#GET speed vehicules by time

ddf=df.groupBy("id","time","pos").avg("speed")



#Automatically identify categorical features, and index them
assembler = VectorAssembler(
    inputCols=["time"],
    outputCol="features")

train02=assembler.transform(ddf)

#SELECT feeatures and label
dataDB = train02.select(col("features").alias("features"),col("avg(speed)").alias("label"))

#split data
(trainingDataReg, testDataReg) =dataDB.randomSplit([0.7, 0.3])

#create linear regression model
lr = LinearRegression()
lrModel = lr.fit(trainingDataReg)


# Make predictions.
dataDB=lrModel.transform(testDataReg)

# select example rows to display.
dataDB.show()


evaluatorReg=RegressionEvaluator()

r2=evaluatorReg.evaluate(dataDB,{evaluatorReg.metricName:"r2"})

r2









#Select lane 
df = df.withColumn("laned", df.lane.substr(-6,2))
df.show()

#create temporary table to use spark SQL
tempTable=df.createOrReplaceTempView("track_vhicules")

#Select vehicule number by lane and time 

VTL = spark.sql("""SELECT time,x,y,pos,laned,sum(id) AS totalVehicules   FROM track_vhicules GROUP BY time,x,y,pos,laned ORDER BY(totalVehicules ) desc""")
VTL.show()


VTL = VTL.withColumn("x", VTL["x"].cast(FloatType()))
VTL = VTL.withColumn("time", VTL["time"].cast(FloatType()))
VTL = VTL.withColumn("y", VTL["y"].cast(FloatType()))

VTL = VTL.withColumn("pos", VTL["pos"].cast(FloatType()))

tempTbl=VTL.createOrReplaceTempView("CongestionPredic")



dataCongestion=spark.sql("""SELECT time,x,y,pos,laned,totalVehicules, CASE WHEN totalVehicules > 8 THEN 'Yes' ELSE 'NO' END AS isCongestion FROM CongestionPredic""")


dataCongestion = dataCongestion.withColumn("laned", VTL["laned"].cast(IntegerType()))



(trainingData, testData) =dataCongestion.randomSplit([0.7, 0.3])


dataCongestion.stat.corr("time","totalVehicules")

assemblerCongestion = VectorAssembler(
    inputCols=["time","totalVehicules"],
    outputCol="features")


indexerR = StringIndexer(inputCol="isCongestion", outputCol="label").fit(dataCongestion)


AssCongest=assemblerCongestion.transform(dataCongestion)

FeatCongs=AssCongest.select("features","isCongestion")





dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")

# Chain indexers and tree in a Pipeline
pipeline = Pipeline(stages=[indexerR, assemblerCongestion, dt])

# Train model.  This also runs the indexers.
modelTree = pipeline.fit(trainingData)



# Make predictions.
predictionsTree =modelTree.transform(testData)

# Select example rows to display.
predictionsTree.select("prediction", "label", "features").show(20)


# Select (prediction, true label) and compute test error
evaluatorTree = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracyTree = evaluatorTree.evaluate(predictionsTree)
accuracyTree




RandomForestModel= RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10)



# Chain indexers and forest in a Pipeline

pipelinee = Pipeline(stages=[indexerR ,assemblerCongestion, RandomForestModel])

# Train model.  This also runs the indexers.
modelForest = pipelinee.fit(trainingData)

# Make predictions.
predictionsForest = modelForest.transform(testData)


predictionsForest.show() 



evaluatorForest = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracyForest = evaluatorForest.evaluate(predictionsForest)

accuracyForest


dataCongestion.stat.corr("time","totalVehicules")

assemblerCongestion = VectorAssembler(
    inputCols=["time","totalVehicules"],
    outputCol="features")


AssCongest=assemblerCongestion.transform(dataCongestion)

FeatCongs=AssCongest.select("features","isCongestion")


indexerR = StringIndexer(inputCol="isCongestion", outputCol="label")


outConges =indexerR.fit(FeatCongs).transform(FeatCongs)


outConges=outConges.select("features","label")


(trainingData, testData) =outConges.randomSplit([0.7, 0.3])


lsvc = LinearSVC(maxIter=5, regParam=0.1)

# Fit the model
lsvcModel = lsvc.fit(trainingData)


predictionsSVC = lsvcModel.transform(testData)
predictionsSVC.show()


evaluatorSVC = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")

accuracySVC = evaluatorSVC.evaluate(predictionsSVC)

accuracySVC












nb = NaiveBayes(smoothing=1.0, modelType="multinomial")



modelNB = nb.fit(trainingData)


predictionsNB = modelNB.transform(testData)
predictionsNB.show()

# compute accuracy on the test set
evaluatorNB = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                              metricName="accuracy")
accuracyNB = evaluatorNB.evaluate(predictionsNB)
print("Test set accuracy = " + str(accuracyNB))

Accuracies =sparkt.createDataFrame([("DecisionTreeClassifier",  accuracyTree), ("RandomForestClassifier",accuracyForest),
                                     ("LinearSVC", accuracySVC),("NaiveBayes", accuracyNB)],["AlgoName", "accuracy"])

Accuracies.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").save()




