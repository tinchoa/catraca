"""
run the example
	`$ bin/spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.1.1.jar kafkaS-spark-packets.py  10.10.10.8:2181 test`
"""
from __future__ import print_function

import sys

from pyspark.mllib.linalg import Vectors
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, OffsetRange
import json

def f(x):
	for j in x:
		for i in range(len(j)):
			j[i]=float(j[i])
	return x

numberFeatures=25

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
		exit(-1)

	
	sc = SparkContext(appName="PythonStreamingKafkaWordCount")
	ssc = StreamingContext(sc, 1)

	###kafka
	zkQuorum, topic = sys.argv[1:]
	kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
	parsed = kvs.map(lambda v: json.loads(v[1]))  

	lines  = parsed.map(lambda x: f([(x[i*numberFeatures:(i+1)*numberFeatures-1]) for i in range(len(x)/numberFeatures)]))

	
	vec=lines.map(lambda x: [Vectors.dense(x[i]) for i in range(len(x)) ]) #now we have the vectors with the format of the ML
		
	vec.pprint()

		
	ssc.start()
	ssc.awaitTermination()
