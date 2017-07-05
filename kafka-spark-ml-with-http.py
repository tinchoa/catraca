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

from py4j.protocol import Py4JJavaError
import os
import subprocess

####for firewall
import httplib
import json



numberFeatures=25 #dataset Antonio=25 dataset com=41
ipFirewall='10.240.114.31'

def convertTofloat(x):
	for j in x:
		for i in range(len(j)):
			j[i]=float(j[i])
	return x


def MatrixReducer(vectors, reducedIndex):
	aux = reducedIndex[0:5] #tacking the first 6 features

	index =[]

	reducedMatrix =[]
	#####
	vectors = np.matrix(vectors)

	for k in aux:
		index.append(k[1])
		#reducedMatrix.append(matrizRaw[:,k[1]]) #reduced matrix 
		reducedMatrix.append(vectors[:,k[1]]) #reduced matrix 


	vectors2 = np.column_stack(reducedMatrix)
	vectors2 = np.array(vectors2)
	
	return vectors2


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

	mat = sc.parallelize(vectors) 	

	matriz = Statistics.corr(mat, method="pearson")
	
	summary = Statistics.colStats(mat)

	varianza = summary.variance()

	#########new heuristic
	w={}
	aij={}
	for i in range(len(matriz)):
		w[i]=0
		aij[i]=0
		for j in np.nan_to_num(matriz[i]):
			k=abs(j)
			aij[i]=aij[i]+k
		w[i]=varianza[i]/aij[i]

	index=sorted([(value,key) for (key,value) in w.items()],reverse=True) #features sorted

	vectors2=MatrixReducer(vectors,index)

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

	for i in range(len(vectors2)):
		if sys.argv[1] == '1': # dataset Renato
			if int(classes[i]) == 0:
				dif1.append(vectors2[i])
				e.append(LabeledPoint(0,np.array(dif1)))
				dif1=[]
			if int(classes[i]) == 1:
				dif2.append(vectors2[i])
				e.append(LabeledPoint(1,np.array(dif2)))
				dif2=[]
		else:
			
			if int(classes[i]) == 1:
				dif1.append(vectors2[i])
				e.append(LabeledPoint(0,np.array(dif1)))
				dif1=[]
			if int(classes[i]) == 2:
				dif2.append(vectors2[i])
				e.append(LabeledPoint(1,np.array(dif2)))
				dif2=[]
			if int(classes[i]) == 3:
				dif3.append(vectors2[i])
				e.append(LabeledPoint(2,np.array(dif3)))
				dif3=[]

	final=sc.parallelize(e)
	
	return final, index	

	


def blockFlows(prediction, vec):
	prediction.map(lambda x: x).pprint()
	#if prediction != 0.0:
		# ipSrc=vec.map(lambda x: [x[i][0] for i in range(len(x)) ])
		# ipDst=vec.map(lambda x: [x[i][2] for i in range(len(x)) ])
		# conn = httplib.HTTPConnection(ipFirewall,8000)
		# conn.request("POST","/add",json.dumps({'ipSrc':ipSrc, 'ipDst':ipDst}))
		# res=conn.getresponse()
		# res.read()


def path_exist(file): #nao esta funcionando
	path='hdfs://master:9000/user/app/'
	try:
		cmd = ['hdfs', 'dfs', '-find',path]
		files = subprocess.check_output(cmd).strip().split('\n')
		if file in files:
			return True
		else:
			return False
	except Py4JJavaError as e:
		return False
		

def getModel():
	
	#if path_exist("hdfs://master:9000/user/app/model-antonioDT.model"):
		
	#	return DecisionTreeModel.load(sc, "hdfs://master:9000/user/app/model-antonioDT.model")

	#else:
		trainingData, index = preparingData(sc.textFile('hdfs://master:9000/user/app/dataset_GTA.csv',5))

		# Train a DecisionTree model.
		#  Empty categoricalFeaturesInfo indicates all features are continuous.
		
		model = DecisionTree.trainClassifier(trainingData, 3,{})
											 #, maxDepth=5, maxBins=32)
	#	model.save(sc, "hdfs://master:9000/user/app/model-antonioDT.model")			

		return	model, index



if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
		exit(-1)

	sc = SparkContext(appName="Kafka with DT")

	#Create model
	
	[model,index]=getModel()
	
	######

	####Streaming
	
	ssc = StreamingContext(sc, 1)

	###kafka
	zkQuorum, topic = sys.argv[1:]

	kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})

	parsed = kvs.map(lambda v: json.loads(v[1]))  

	lines  = parsed.map(lambda x: convertTofloat([(x[i*numberFeatures:(i+1)*numberFeatures-1]) for i in range(len(x)/numberFeatures)]))

	vec = lines.map(lambda x: [Vectors.dense(x[i]) for i in range(len(x)) ]) #now we have the vectors with the format of the ML
	
	test = vec.map(lambda x: MatrixReducer(x,index))

	#prediction=vec.transform(lambda _, rdd: model.predict(rdd))

	prediction = test.transform(lambda _, rdd: model.predict(rdd))


	blockFlows(prediction,vec)

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
