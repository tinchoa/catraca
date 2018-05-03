'''
 pyspark --master spark://master:7077 --conf spark.executor.extraJavaOptions=" -XX:MaxPermSize=15G " --executor-memory 10G --driver-memory 5G --conf spark.driver.maxResultSize=5g


'''


import sys
import numpy as np
import json

from pyspark.mllib.stat import Statistics
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils
from tempfile import NamedTemporaryFile


'''
bin/spark-submit  --master spark://master:7077 feature-selection.py  <1= dataset Renato> hdfs://master:9000/user/app/classes-25.out

'''

numberFeatures=46 # Should be every analysed feature; that is, all-4 (ipsrc,portsrc,ipdest,portdst)
numberClasses=2   # For dataset Antonio: 0=Normal, 1=DoS, 2=Probe | Renato: 0=Normal 1=Alert
classes=[]


def dataPreparing(lines):

	virgulas  = lines.map(lambda x: x.split(',')).map(lambda x:(json.dumps(x[0:4]), x[4:numberFeatures])) #vamos fazer uma tupla ips,todas as caract
	vectors = virgulas.mapValues(lambda x: np.array(x)) #convertir os values em arrays
	test = vectors.map(lambda x:x[1]) #take so os values
	classes = test.map(lambda x:x[numberFeatures-5]) #get the class
	classes=classes.map(lambda x: '1' if x !='0' else '0') # passing to binary classes
	test = test.map(lambda x:x[0:numberFeatures-5]) #removing the class
	
	print 'processing data'	

	return test, classes ####ver como llega este test


def CorrelationFeature(vectors):

	
	print 'Calculation Correlation'
	
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

	index=r[0:6] #tacking the first 6 features

	red=MatrixReducer(vectors,index)
	return red



def MatrixReducer(vector,index):

	def takeElement(vector):
		p=[]
		for i in index:
			p.append(vector[i[1]])
		return p
	
	reducedMatrix= vector.map(lambda x: takeElement(x))
	#print 'reducing matrix'

	# for k in aux:
	# 	index.append(k[1])
	# 	#reducedMatrix.append(matrizRaw[:,k[1]]) #reduced matrix 
	# 	reducedMatrix.append(vectors[:,k[1]]) #reduced matrix 


	vectors2=reducedMatrix.map(lambda x: np.column_stack(x))

	# vectors2= np.column_stack(reducedMatrix)
	# vectors2= np.array(vectors2)

	return vectors2 #matriz reducida

def pass2libsvm(vectors2,classes):

	newVector=classes.zip(vectors2)
	grouped=newVector.groupByKey().mapValues(list)
	final=newVector.map(lambda x : LabeledPoint(x[0],x[1]))


	
	print 'returning libsvm format'
	# final=sc.parallelize(e) #return in libsvm format

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
	# spark = SparkSession\
	# 	.builder\
	# 	.appName("DecisionTreeClassificationExample")\
	# 	.getOrCreate()

	
	sc = SparkContext(appName="5-tuple Features")


#data=CorrelationFeature(sc.textFile('hdfs://master:9000/user/app/reduced-25.out',5))

	#vector,classes=dataPreparing(sc.textFile(sys.argv[1]))
	#file='hdfs://master:9000/user/app/1percent-5tuple-features-sem-smurf.csv'
	file =sys.argv[1]

	vector,classes=dataPreparing(sc.textFile(file))


	reduced=CorrelationFeature(vector) #se precisar de feature do Feature Selection

	#data=pass2libsvm(vector) 

	data=pass2libsvm(reduced,classes) 

	#data=pass2libsvm(vector,classes) 


	#para a (5-tupla deveria ser algo como ) data=pass2libsvm(vector)

	(trainingData, testData) = data.randomSplit([0.7, 0.3])
	print 'dataset divided in train and test'

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

	#print predictions.take(20)

	labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)

	metrics = MulticlassMetrics(labelsAndPredictions)

	testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())

	print('Learned classification tree model:')
	print(model.toDebugString())



	#tp=metrics.truePositiveRate(1.0)
	#fp=metrics.falsePositiveRate(0.0)
	acuracy = metrics.accuracy
	precision = metrics.precision()
	recall = metrics.recall()
	f1Score = metrics.fMeasure()
	confusionMatrix = metrics.confusionMatrix().toArray()
	print("Summary Stats")
	#print('True Positive Rate = %s' % tp)
	#print('False Positive Rate = %s' % fp)
	print('Acuracy = %s' % acuracy)
	print('Test Error = ' + str(testErr))
	print("Precision = %s" % precision)
	print("Recall = %s" % recall)
	print("F1 Score = %s" % f1Score)
	print("confusionMatrix = %s" % confusionMatrix)


	file='hdfs://master:9000/user/app/Results_'+str(file).split('/app')[1].split('/')[1].split('.csv')[0]
	sc.parallelize([metrics.accuracy, metrics.precision(), metrics.recall(),metrics.fMeasure(), metrics.confusionMatrix()]).saveAsTextFile(file)

	## comand to get file hdfs dfs -getmerge hdfs://master:9000/user/app/Results_1percent-5tuple-features-sem-smurf/* /tmp/test/file.txt



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

