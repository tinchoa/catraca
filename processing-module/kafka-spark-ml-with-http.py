"""
run the example
	bin/spark-submit --master spark://master:7077 --conf spark.executor.extraJavaOptions=" -XX:MaxPermSize=15G " --jars spark-streaming-kafka-0-8-assembly_2.11-2.1.1.jar  new-with-http.py hdfs://master:9000/user/app/reduced-25-with-classes.out 10.10.10.3:2181 topic1
    

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




numberFeatures=46 #dataset Antonio=25 dataset com=41
ipFirewall='10.240.114.45'
#31'
numberClasses=2 #for dataset Antonio (0=Normal, 1=DoS, 2=Probe (3 classes)) #renato 0=Normal 1=Alerta (2 classes)


def convertTofloat(x):
	for i in range(len(x)):
			x[i]=float(x[i])
	return x

def convertToString(x):
	for i in range(len(x)):
		x[i]=str(x[i])
	return x


def dataPreparing(lines):

	virgulas  = lines.map(lambda x: x.split(',')).map(lambda x:(json.dumps(x[0:4]), x[4:numberFeatures])) #vamos fazer uma tupla ips,todas as caract
	vectors = virgulas.mapValues(lambda x: np.array(x)) #convertir os values em arrays
	test = vectors.map(lambda x:x[1]) #take so os values
	classes = test.map(lambda x:x[numberFeatures-5]) #get the class
	test = test.map(lambda x:x[0:numberFeatures-5]) #removing the class
	
	#print 'processing data'	

	return test, classes ####ver como llega este test


def CorrelationFeature(vectors):

	
#	print 'Calculation Correlation'
	
	matriz=sc.broadcast(Statistics.corr(vectors, method="pearson"))

	summary = Statistics.colStats(vectors)

	varianza=summary.variance()


	#########new heuristic diogo proposal
	w={}
	aij={}
	for i in range(len(matriz.value)):
		w[i]=0
		aij[i]=0
		for j in np.nan_to_num(matriz.value[i]):
			k=abs(j)
			aij[i]=aij[i]+k
		w[i]=varianza[i]/aij[i]

	r=sorted([(value,key) for (key,value) in w.items()],reverse=True) #features sorted

	#print r

#	print 'calculating features selections'

	#Old heuristic
	# # w={}
	# # for i in range(len(matriz)):
	# # 	w[i]=0
	# # 	for j in np.nan_to_num(matriz[i]):
	# # 		k=abs(j)
	# # 		w[i]=w[i]+k

	# r=sorted([(value,key) for (key,value) in w.items()],reverse=True)

	


	#####""
	#vectors=np.matrix(vectors)
	#beforeMatrix=vectors.map(lambda x: np.matrix(x))

	index=[]
	for i in r:
		index.append(i[1])
	
	index=index[0:6] #tacking the first 6 features

	#MatrixReducer(vectors,index)
	return index



# def MatrixReducer(vectors,index):

# 	def takeElement(vector):
# 		p=[]
# 		for i in index:
# 			p.append(vector[i])
# 		return p
	
# 	reducedMatrix= vectors.map(lambda x: takeElement(x))
# 	#print 'reducing matrix'

# 	# for k in aux:
# 	# 	index.append(k[1])
# 	# 	#reducedMatrix.append(matrizRaw[:,k[1]]) #reduced matrix 
# 	# 	reducedMatrix.append(vectors[:,k[1]]) #reduced matrix 


# 	vectors2=reducedMatrix.map(lambda x: np.column_stack(x))

# 	# vectors2= np.column_stack(reducedMatrix)
# 	# vectors2= np.array(vectors2)

# 	return vectors2 #matriz reducida


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


def pass2libsvm(vectors2,classes):

	newVector=classes.zip(vectors2)
	grouped=newVector.groupByKey().mapValues(list)
	final=newVector.map(lambda x : LabeledPoint(x[0],x[1]))


	# ###to make the reduced matrix with vectors
	# dif1=[]
	# #dif1 = [0]*len(vectors)
	# z={}
	# z[1]=[]
	# dif2=[]
	# #dif2 = [0]*len(vectors)
	# z[2]=[]

	# dif3=[]
	# z[3]=[]
	# #dif3 = [0]*len(vectors)
	# e=[]
	# for i in range(len(vectors2)):
	# 		if int(classes[i]) == 0:
	# 			dif1.append(vectors2[i])
	# 			e.append(LabeledPoint(0,np.array(dif1)))
	# 			dif1=[]
	# 		if int(classes[i]) == 1:
	# 			dif2.append(vectors2[i])
	# 			e.append(LabeledPoint(1,np.array(dif2)))
	# 			dif2=[]
	# 		if int(classes[i]) == 2:
	# 			dif3.append(vectors2[i])
	# 			e.append(LabeledPoint(2,np.array(dif3)))
	# 			dif3=[]
		
	# 	#ver como hacer el tema de la libsvm list
	# 	#deveria ser algo del tipo 1, () ,2 (), 1 (), 3 (), 2()

	#print 'returning libsvm format'
	# final=sc.parallelize(e) #return in libsvm format

	return final



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
		

def getModel(path,file):
	
	if path_exist(path+'index-'+file):
		index=sc.textFile(path+'index-'+file)
		a=index.collect()
		b=lambda x : [ int(i) for i in x ]
		
		return DecisionTreeModel.load(sc, path+'model-'+file), b(a)


	else:

		vector,classes = dataPreparing(sc.textFile(path+file))

		index=CorrelationFeature(vector) #se precisar de feature do Feature Selection

		reduced=MatrixReducer(vector,index) 

		#data=pass2libsvm(vector) 

		data=pass2libsvm(reduced,classes) 

	#data=pass2libsvm(vector,classes) 


	#para a (5-tupla deveria ser algo como ) data=pass2libsvm(vector)

		#(trainingData, testData) = data.randomSplit([0.7, 0.3])

		# Train a DecisionTree model.
		#  Empty categoricalFeaturesInfo indicates all features are continuous.
		
		model = DecisionTree.trainClassifier(data, numberClasses,{})	 #, maxDepth=5, maxBins=32)

		model.save(sc, path+'model-'+file)			

		return	model, index


if __name__ == "__main__":
	if len(sys.argv) != 4:
		print("Usage: kafka_wordcount.py <file> <hdfs-files> <zk> <topic> ", file=sys.stderr)
		exit(-1)

	sc = SparkContext(appName="Kafka with DT")

	#Create model
	a=0
	orig=sys.argv[1]
	path='hdfs://master:9000/user/app/'
	file=orig.split('app/')[1]

	[model,index]=getModel(path,file)
	
	if path_exist(path+'index-'+file) == False: #hdfs://master:9000/user/app/index-25-reduced.txt') == False:
		rdd=sc.parallelize(index)
		rdd.saveAsTextFile(path+'index-'+file)#'hdfs://master:9000/user/app/index-25-reduced.txt')

	####Streaming
	
	ssc = StreamingContext(sc, 1)


	###kafka
	zkQuorum, topic = sys.argv[2:]

	kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})

	parsed = kvs.map(lambda v: json.loads(v[1]))  


#	def preparaAporraToda(parsed):

	lines  = parsed.map(lambda x: x.split(',')).map(lambda x:(json.dumps(x[0:4]), x[4:numberFeatures-1])).mapValues(lambda x: convertTofloat(x))
		
#		lines= lines.reduceByKey()
	#lines.pprint()

 	test = lines.flatMapValues(lambda x: MatrixReducer(x,index))

		   
	vec = test.mapValues( Vectors.dense) #now we have the vectors with the format of the ML
		

	#	return test


	#a=preparaAporraToda(parsed)

	# def saveHDFS(rdd2,a):
	# 	if a==0:
	# 		b=[]
	# 	if a <=10:
	# 		b.append(rdd2)
	# 		a+=1
	# 	else:
	# 		rdd2.saveAsTextFile('hdfs://master:9000/user/app/joined.txt')
	# 		a=0
	# 		b=[]
	# 	return a

	try:	
		vec=test.map(lambda x: x[1])
		ips=test.transform(lambda x: x.keys().zipWithIndex()).map(lambda x: (x[1],x[0]))
#		prediction=test.transform(lambda x: model.predict(x.values())).pprint()
		algo=test.transform(lambda x: model.predict(x.values()).zipWithIndex()).map(lambda x: (x[1],x[0]))
#		ips.foreachRDD(lambda v: print(v.collect()))
#		algo.foreachRDD(lambda v: print(v.collect()))
		joined = ips.join(algo).transform(lambda x: x.values())
		#a=saveHDFS(joined,a)
		joined.foreachRDD(lambda v: print(v.collect()))
		joined.map(blockFlows).pprint()

#		algo=sc.parallelize(ips.collect(),prediction.collect()).pprint()

#	        test.map(lambda x: (x[0],retornaRDD(x[1]))).pprint()
	except AttributeError:
		pass

	ssc.start()
	ssc.awaitTermination()
