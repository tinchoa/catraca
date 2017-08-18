from __future__ import print_function

# $example on$
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# $example off$
from pyspark.sql import SparkSession


from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import StreamingKMeans


import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


if __name__ == "__main__":
	spark = SparkSession\
		.builder\
		.appName("DecisionTreeClassificationExample")\
		.getOrCreate()



		#sc = SparkContext(appName="PythonStreamingKafkaWordCount")
	sc = spark.spark
	ssc = StreamingContext(sc, 1)

	#trainingData = sc.textFile("data/mllib/kmeans_data.txt")\
	trainingData = sc.textFile('hdfs://master:9000/user/app/dataset_GTA.csv',5).map(lambda line: Vectors.dense([float(x) for x in line.strip().split(' ')]))

	#testingData = sc.textFile("data/mllib/streaming_kmeans_data_test.txt").map(parse)

	###kafka~~
	zkQuorum, topic = sys.argv[1:]
	kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
	lines = kvs.map(lambda x: x[1])
	vec = Vectors.dense(lines[lines.find('[') + 1: lines.find(']')].split(','))
	#counts = lines.flatMap(lambda line: line.split("\\n")) \
		#.map(lambda word: (word, 1)) \
		#.reduceByKey(lambda a, b: a+b)
	vec.pprint()
	#####

	testingData=counts
	
	trainingQueue = [trainingData]
	testingQueue = [testingData]

	trainingStream = ssc.queueStream(trainingQueue)
	testingStream = ssc.queueStream(testingQueue)

	# We create a model with random clusters and specify the number of clusters to find
	model = StreamingKMeans(k=2, decayFactor=1.0).setRandomCenters(3, 1.0, 0)

	# Now register the streams for training and testing and start the job,
	# printing the predicted cluster assignments on new data points as they arrive.
	model.trainOn(trainingStream)

	result = model.predictOnValues(testingStream.map(lambda lp: (lp.label, lp.features)))
	result.pprint()

	ssc.start()
	ssc.stop(stopSparkContext=True, stopGraceFully=True)