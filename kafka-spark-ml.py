"""
run the example
	`$ bin/spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.1.1.jar kafkaS-spark-packets.py  10.10.10.3:2181 test2`
"""
from __future__ import print_function

import sys

from pyspark.mllib.linalg import Vectors
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, OffsetRange
import json

#### ML
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils
####


####
import numpy as np
from pyspark.mllib.stat import Statistics
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils
from tempfile import NamedTemporaryFile
####

def f(x):
	for j in x:
		for i in range(len(j)):
			j[i]=float(j[i])
	return x

numberFeatures=25

def preparingData(data):

	virgulas=data.map(lambda x:x.split(','))

	aux=virgulas.take(virgulas.count())

	algo=sc.broadcast(aux) #make the variable broadcast is gonna let us work just with 'pointers'

	vectors=[]

	classes=[]

	matrizRaw=[]

	for i in algo.value:
		vectors.append(np.array(i))
		matrizRaw.append(i)
		classes.append(i[len(i)-1])

	matrizRaw=np.matrix(matrizRaw) #matrix with raw values

	for m in range(len(vectors)):
		vectors[m]=np.delete(vectors[m],numberFeatures-1,0) #deleting the class

	###to make the reduced matrix with vectors
	dif1=[]
	#dif1 = [0]*len(vectors)
	z={}
	z[1]=[]
	dif2=[]
	#dif2 = [0]*len(vectors)
	z[2]=[]

	dif3=[]
	z[3]=[]
	#dif3 = [0]*len(vectors)
	e=[]



	for i in range(len(vectors)):
		if int(classes[i]) == 1:
			dif1.append(vectors[i])
			e.append(LabeledPoint(0,np.array(dif1)))
			dif1=[]
		if int(classes[i]) == 2:
			dif2.append(vectors[i])
			e.append(LabeledPoint(1,np.array(dif2)))
			dif2=[]
		if int(classes[i]) == 3:
			dif3.append(vectors[i])
			e.append(LabeledPoint(2,np.array(dif3)))
			dif3=[]


	#ver como hacer el tema de la libsvm list
	#deveria ser algo del tipo 1, () ,2 (), 1 (), 3 (), 2()

	final=sc.parallelize(e) #return in libsvm format


	return final	



if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
		exit(-1)

	sc = SparkContext(appName="PythonStreamingKafkaWordCount")

	#Create model
	trainingData = preparingData(sc.textFile('hdfs://master:9000/user/app/dataset_GTA.csv',5))

	# Train a DecisionTree model.
	#  Empty categoricalFeaturesInfo indicates all features are continuous.
	
	model = DecisionTree.trainClassifier(trainingData, 3,{})
										 #, maxDepth=5, maxBins=32)

	


	######

	####Streaming
	
	ssc = StreamingContext(sc, 1)

	###kafka
	zkQuorum, topic = sys.argv[1:]
	kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
	parsed = kvs.map(lambda v: json.loads(v[1]))  

	lines  = parsed.map(lambda x: f([(x[i*numberFeatures:(i+1)*numberFeatures-1]) for i in range(len(x)/numberFeatures)]))

	
	vec=lines.map(lambda x: [Vectors.dense(x[i]) for i in range(len(x)) ]) #now we have the vectors with the format of the ML
		
	vec.transform(lambda _, rdd: model.predict(rdd)).pprint()

	#vec.transform(lambda _, rdd: model.predict(rdd)).pprint()

	######Evaluation

	# Evaluate model on test instances and compute test error
	# predictions = model.predict(vec.map(lambda x: x.features))
	# labelsAndPredictions = vec.map(lambda lp: lp.label).zip(predictions)
	# testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(vec.count())
	# print('Test Error = ' + str(testErr))
	# print('Learned classification tree model:')
	# print(model.toDebugString())







		
	ssc.start()
	ssc.awaitTermination()
