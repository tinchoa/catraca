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




numberFeatures=45 #dataset Antonio=25 dataset com=41
ipFirewall='10.240.114.45'
#31'
numberClasses=2 #for dataset Antonio (0=Normal, 1=DoS, 2=Probe) #renato 0=Normal 1=Alerta


def convertTofloat(x):
	for i in range(len(x)):
			x[i]=float(x[i])
	return x

def convertToString(x):
	for i in range(len(x)):
		x[i]=str(x[i])
	return x


def MatrixReducer(vectors, index):
	#index=sc.textFile("hdfs://master:9000/user/app/index-25-reduced.txt")


#	aux = index[0:5] #tacking the first 6 features

	reducedMatrix =[]
	#####
	vectors = np.matrix(vectors)

	for k in index:
		#reducedMatrix.append(matrizRaw[:,k[1]]) #reduced matrix 
		reducedMatrix.append(vectors[:,k]) #reduced matrix 

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
		if sys.argv[1] == '1': # dataset Renato
				i = np.delete(i,0,0) #IPsrc
				i = np.delete(i,0,0) #PortSrc 
				i = np.delete(i,0,0) #IPdst 
				i = np.delete(i,0,0) #Portdst 
				if (i[numberFeatures-5] != u'0'):
					i[numberFeatures-5] = u'1'
		vectors.append(np.array(i))
		matrizRaw.append(i)
		classes.append(i[numberFeatures-5])

	#print 'matriz Raw done'
	matrizRaw=np.matrix(matrizRaw) #matrix with raw values

	for m in range(len(vectors)):
		vectors[m]=np.delete(vectors[m],numberFeatures-5,0) #deleting the class

	mat = sc.parallelize(vectors) 	

	matriz = Statistics.corr(mat, method="pearson")
	
	summary = Statistics.colStats(mat)

	varianza = summary.variance()


	#Feature Selection
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
	
	k=[]
	for i in index:
		k.append(i[1])

	vectors2=MatrixReducer(vectors,k[0:5])

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
	
	return final, k[0:5]


def blockFlows(flow):
	vec = flow[0]
	prediction = flow[1]
	#prediction.map(lambda x: x).pprint()
	if prediction != 0.0:
                tupla = json.loads(vec)
		ipSrc=tupla[0]
#		ipSrc.map(lambda x: str(x)).pprint()
#		ipDst=vec.map(lambda x: x.split(',')).map(lambda x: x[2])
		ipDst=tupla[2]
		conn = httplib.HTTPConnection(ipFirewall,8000)
		conn.request("POST","/add",json.dumps({'ipSrc':ipSrc, 'ipDst':ipDst}))
		res=conn.getresponse()
		res.read()
	return flow


def path_exist(file): 
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
	
	if path_exist("hdfs://master:9000/user/app/model25-reduced.model"):
		index=sc.textFile("hdfs://master:9000/user/app/index-25-reduced.txt")
		a=index.collect()
		b=lambda x : [ int(i) for i in x ]
		
		return DecisionTreeModel.load(sc, "hdfs://master:9000/user/app/model25-reduced.model"), b(a)


	else:
		trainingData, index = preparingData(sc.textFile('hdfs://master:9000/user/app/reduced25-classes.out',5))

		# Train a DecisionTree model.
		#  Empty categoricalFeaturesInfo indicates all features are continuous.
		
		model = DecisionTree.trainClassifier(trainingData, numberClasses,{})	 #, maxDepth=5, maxBins=32)

		model.save(sc, "hdfs://master:9000/user/app/model25-reduced.model")			

		return	model, index


if __name__ == "__main__":
	if len(sys.argv) != 4:
		print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
		exit(-1)

	sc = SparkContext(appName="Kafka with DT")

	#Create model

	[model,index]=getModel()
	
	if path_exist('hdfs://master:9000/user/app/index-25-reduced.txt') == False:
		rdd=sc.parallelize(index)
		rdd.saveAsTextFile('hdfs://master:9000/user/app/index-25-reduced.txt')

	####Streaming
	
	ssc = StreamingContext(sc, 1)


	###kafka
	zkQuorum, topic = sys.argv[2:]

	kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})

	parsed = kvs.map(lambda v: json.loads(v[1]))  

<<<<<<< HEAD

#	def preparaAporraToda(parsed):

        lines  = parsed.map(lambda x: x.split(',')).map(lambda x:(json.dumps(x[0:4]), x[4:numberFeatures])).mapValues(lambda x: convertTofloat(x))
		
#		lines= lines.reduceByKey()
=======
    	lines  = parsed.map(lambda x: x.split(',')).map(lambda x:(json.dumps(x[0:4]), x[4:numberFeatures])).mapValues(lambda x: convertTofloat(x))
>>>>>>> 338486083b8e947f28379c3befe7ef96836e43c7

	test = lines.flatMapValues(lambda x: MatrixReducer(x,index))

	       
	vec = test.mapValues( Vectors.dense) #now we have the vectors with the format of the ML
		

	#	return test


	#a=preparaAporraToda(parsed)



	try:	
		vec=test.map(lambda x: x[1])
		ips=test.transform(lambda x: x.keys().zipWithIndex()).map(lambda x: (x[1],x[0]))
#		prediction=test.transform(lambda x: model.predict(x.values())).pprint()
		algo=test.transform(lambda x: model.predict(x.values()).zipWithIndex()).map(lambda x: (x[1],x[0]))
#		ips.foreachRDD(lambda v: print(v.collect()))
#		algo.foreachRDD(lambda v: print(v.collect()))
		joined = ips.join(algo).transform(lambda x: x.values())
		joined.foreachRDD(lambda v: print(v.collect()))
		joined.map(blockFlows).pprint()
#		algo=sc.parallelize(ips.collect(),prediction.collect()).pprint()

#	        test.map(lambda x: (x[0],retornaRDD(x[1]))).pprint()
	except AttributeError:
		pass

	ssc.start()
	ssc.awaitTermination()
