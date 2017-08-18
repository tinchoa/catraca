




   conf = {"hbase.zookeeper.quorum": host,
13:        "hbase.mapred.outputtable": table,
14:        "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
15:        "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
16:        "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
