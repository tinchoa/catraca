import numpy as np
from pyspark.mllib.stat import Statistics
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils
from tempfile import NamedTemporaryFile
from sklearn.cluster import KMeans


from pyspark.mllib.linalg import Vectors
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


numberFeatures=25
#lines = sc.textFile('hdfs://master:9000/user/app/dataset_GTA.csv',5)
numberClasses=2 #for dataset Antonio (0=Normal, 1=DoS, 2=Probe)

def CorrelationFeature(lines):

	virgulas=lines.map(lambda x:x.split(','))

	broad=virgulas.take(virgulas.count())

	algo=sc.broadcast(broad) #make the variable broadcast is gonna let us work just with 'pointers'

	vectors=[]

	classes=[]

	matrizRaw=[]

	for i in algo.value:
		if sys.argv[1] == '1': # dataset Renato
				i = np.delete(i,0,0) #IPsrc
				i = np.delete(i,0,0) #PortSrc 
				i = np.delete(i,0,0) #IPdst 
				i = np.delete(i,0,0) #Portdst 
				if (i[numberFeatures-1] != u'0'):
					i[numberFeatures-1] = u'1'
		vectors.append(np.array(i))
		matrizRaw.append(i)
		classes.append(i[numberFeatures-1])

	matrizRaw=np.matrix(matrizRaw) #matrix with raw values



	#to remove  features
	for m in range(len(vectors)):
		vectors[m]=np.delete(vectors[m],numberFeatures-1,0) #deleting the class
		
	# 	vectors[m]=np.delete(vectors[m],19,0) #empty 
	# 	vectors[m]=np.delete(vectors[m],19,0) #empty
	# ###################################

	mat = sc.parallelize(vectors) 	

	matriz=Statistics.corr(mat, method="pearson")

	summary = Statistics.colStats(mat)

	varianza=summary.variance()


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

	r=sorted([(value,key) for (key,value) in w.items()],reverse=True) #features sorted

	#Old heuristic
	# # w={}
	# # for i in range(len(matriz)):
	# # 	w[i]=0
	# # 	for j in np.nan_to_num(matriz[i]):
	# # 		k=abs(j)
	# # 		w[i]=w[i]+k

	# r=sorted([(value,key) for (key,value) in w.items()],reverse=True)

	aux=r[0:5] #tacking the first 6 features

	index=[]

	reducedMatrix=[]


	#####""
	vectors=np.matrix(vectors)

	for k in aux:
		index.append(k[1])
		#reducedMatrix.append(matrizRaw[:,k[1]]) #reduced matrix 
		reducedMatrix.append(vectors[:,k[1]]) #reduced matrix 


	vectors2= np.column_stack(reducedMatrix)
	vectors2=np.array(vectors2)

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


	#ver como hacer el tema de la libsvm list
	#deveria ser algo del tipo 1, () ,2 (), 1 (), 3 (), 2()

	final=sc.parallelize(e) #return in libsvm format


	return final

#to save file in disk

#tempFile = NamedTemporaryFile(delete=True)
#tempFile.close()
#MLUtils.saveAsLibSVMFile(sc.parallelize(final), 'hdfs://master:9000/user/app/dataset_GTA.csv')


#prepare the data for the libsvm


####
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils


if __name__ == "__main__":
	spark = SparkSession\
		.builder\
		.appName("DecisionTreeClassificationExample")\
		.getOrCreate()



		#sc = SparkContext(appName="PythonStreamingKafkaWordCount")
	sc = spark.spark
	# Load and parse the data file into an RDD of LabeledPoint.
	#data = MLUtils.loadLibSVMFile(sc, 'data/mllib/sample_libsvm_data.txt')
	# Split the data into training and test sets (30% held out for testing)
	#(trainingData, testData) = final.randomSplit([0.7, 0.3])

	data=CorrelationFeature(sc.textFile('hdfs://master:9000/user/app/dataset_GTA.csv',5))

	(trainingData, testData) = data.randomSplit([0.7, 0.3])
	# trainingData = CorrelationFeature(sc.textFile('hdfs://master:9000/user/app/classes-16.out',15))

	# testData = CorrelationFeature(sc.textFile('hdfs://master:9000/user/app/classes-25.out',15))

	# Train a DecisionTree model.
	#  Empty categoricalFeaturesInfo indicates all features are continuous.
	model = DecisionTree.trainClassifier(trainingData, numberClasses,{})
								 #, maxDepth=5, maxBins=32)

	# Evaluate model on test instances and compute test error
	predictions = model.predict(testData.map(lambda x: x.features))
	labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
	testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
	print('Test Error = ' + str(testErr))
	print('Learned classification tree model:')
	print(model.toDebugString())

	# Save and load model
	# model.save(sc, "target/tmp/myDecisionTreeClassificationModel")
	# sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeClassificationModel")

	##### desicion tree with ML ###deveria ser assim
