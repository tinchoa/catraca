
import numpy as np
from pyspark.mllib.stat import Statistics
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils
from tempfile import NamedTemporaryFile
import sys
from sklearn.cluster import KMeans

'''
bin/spark-submit  --master spark://master:7077 feature-selection.py  <1= dataset Renato> hdfs://master:9000/user/app/classes-25.out

'''

#numberFeatures=46
numberFeatures=42
numberClasses=2 #for dataset Antonio (0=Normal, 1=DoS, 2=Probe) #renato 0=Normal 1=Alerta

def dataPreparing(lines):

	virgulas=lines.map(lambda x:x.split(','))

	broad=virgulas.take(virgulas.count())

	algo=sc.broadcast(broad) #make the variable broadcast is gonna let us work just with 'pointers'

	vectors=[]

	classes=[]

	matrizRaw=[]
	
	#alterei dentro do for
	for i in algo.value:
		#if sys.argv[1] == '1': # dataset Renato
		i = np.delete(i,0,0) #IPsrc
		i = np.delete(i,0,0) #PortSrc 
		i = np.delete(i,0,0) #IPdst 
		i = np.delete(i,0,0) #Portdst 
		if (i[numberFeatures-1] != u'0'):
			i[numberFeatures-1] = u'1'
		vectors.append(np.array(i))
		matrizRaw.append(i)
		classes.append(i[numberFeatures-1])

	print 'matriz Raw done'
	matrizRaw=np.matrix(matrizRaw) #matrix with raw values


	return vectors,classes

def CorrelationFeature(vectors):

	#alterei aqui
	#to remove  features
	#for m in range(len(vectors)):
	#	vectors[m]=np.delete(vectors[m],numberFeatures-1,0) #deleting the class
		
	# 	vectors[m]=np.delete(vectors[m],19,0) #empty 
	# 	vectors[m]=np.delete(vectors[m],19,0) #empty
	# ###################################

	print 'features deleted'
	mat = sc.parallelize(vectors) 	

	matriz=sc.broadcast(Statistics.corr(mat, method="pearson"))
	#matriz=Statistics.corr(mat, method="pearson")

	summary = Statistics.colStats(mat)

	varianza=summary.variance()

	#new heuristic with cluster
	# X=np.array(np.nan_to_num(matriz))
	# kmeans = KMeans(n_clusters=6, random_state=0).fit(X) #6 clusters


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

	print r

	print 'calculating features selections'

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

	print 'reducing matrix'

	vectors2= np.column_stack(reducedMatrix)
	vectors2=np.array(vectors2)

	return vectors2 #matriz reducida

def pass2libsvm(vectors2,classes):
	vectorRDD = sc.parallelize(vectors2) #alterei aqui
	newVector=classes.zip(vectorRDD)
	grouped=newVector.groupByKey().mapValues(list)
	final=newVector.map(lambda x : LabeledPoint(x[0],x[1]))

	print 'returning libsvm format'

	return final
#to save file in disk

#tempFile = NamedTemporaryFile(delete=True)
#tempFile.close()
#MLUtils.saveAsLibSVMFile(sc.parallelize(final), 'hdfs://master:9000/user/app/dataset_GTA.csv')


#prepare the data for the libsvm


####
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils
from pyspark import SparkContext



from pyspark.mllib.evaluation import MulticlassMetrics

# Load and parse the data file into an RDD of LabeledPoint.
#data = MLUtils.loadLibSVMFile(sc, 'data/mllib/sample_libsvm_data.txt')
# Split the data into training and test sets (30% held out for testing)
#(trainingData, testData) = final.randomSplit([0.7, 0.3])

if __name__ == "__main__":
#	spark = SparkSession\
#		.builder\
#		.appName("DecisionTreeClassificationExample")\
#		.getOrCreate()

	
	sc = SparkContext(appName="Feature Selection")


	#data=CorrelationFeature(sc.textFile('hdfs://master:9000/user/app/reduced-25.out',5))

	vector,classes=dataPreparing(sc.textFile('hdfs://master:9000/user/app/reduced-25.out',5))

	reduced=CorrelationFeature(vector) #se precisar de feature do Feature Selection

	data=pass2libsvm(reduced,sc.parallelize(classes))

	#para a (5-tupla deveria ser algo como ) data=pass2libsvm(vector)



	(trainingData, testData) = data.randomSplit([0.7, 0.3])
	print 'data devided'

	#trainingData = CorrelationFeature(sc.textFile('hdfs://master:9000/user/app/classes-16.out',15))

	#testData = CorrelationFeature(sc.textFile('hdfs://master:9000/user/app/classes-25.out',15))

	# Train a DecisionTree model.
	#  Empty categoricalFeaturesInfo indicates all features are continuous.
	model = DecisionTree.trainClassifier(trainingData, numberClasses,{})
										 #, maxDepth=5, maxBins=32)

	# let lrm be a LogisticRegression Model
	
	#model.save(sc, "hdfs://master:9000/user/app/model-"+str(sys.argv[2]+".model"))
	print 'model done'
	#to load the model
	#sameModel = DecisionTreeModel.load(sc, "lrm_model.model")

	# Evaluate model on test instances and compute test error
	predictions = model.predict(testData.map(lambda x: x.features))

	labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)

	metrics = MulticlassMetrics(labelsAndPredictions)

	testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
	
	print('Learned classification tree model:')
	print(model.toDebugString())

	print 'metrics'
	tp=metrics.truePositiveRate(1.0)
	fp=metrics.falsePositiveRate(0.0)
	acuracy = metrics.accuracy()
	precision = metrics.precision()
	recall = metrics.recall()
	f1Score = metrics.fMeasure()
	confusionMatrix = metrics.confusionMatrix().toArray()
	print("Summary Stats")
	print('True Positive Rate = %s' % tp)
	print('False Positive Rate = %s' % fp)
	print('Acuracy = %s' % acuracy)
	print('Test Error = ' + str(testErr))
	print("Precision = %s" % precision)
	print("Recall = %s" % recall)
	print("F1 Score = %s" % f1Score)
	print("confusionMatrix = %s" % confusionMatrix)



	# def printMetrics(predictions_and_labels):
 #    metrics = MulticlassMetrics(predictions_and_labels)
 #    print 'Precision of True ', metrics.precision(1)
 #    print 'Precision of False', metrics.precision(0)
 #    print 'Recall of True    ', metrics.recall(1)
 #    print 'Recall of False   ', metrics.recall(0)
 #    print 'F-1 Score         ', metrics.fMeasure()
 #    print 'Confusion Matrix\n', metrics.confusionMatrix().toArray()

	# # Save and load model
	# model.save(sc, "target/tmp/myDecisionTreeClassificationModel")
	# sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeClassificationModel")

	##### desicion tree with ML ###deveria ser assim
