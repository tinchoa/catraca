#! /bin/bash

/opt/hadoop/sbin/start-all.sh
/opt/hadoop/sbin/start-yarn.sh
/opt/hadoop/sbin/mr-jobhistory-daemon.sh start historyserver
/opt/spark/sbin/start-all.sh
/opt/hbase/bin/start-hbase.sh
