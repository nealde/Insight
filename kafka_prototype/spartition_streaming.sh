#!/bin/bash

#spark-submit --master spark://ip-10-0-0-8:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 --jars /usr/lib/spark/lib/datanucleus-api-jdo-3.2.6.jar,/usr/lib/spark/lib/datanucleus-core-3.2.10.jar,/usr/lib/spark/lib/datanucleus-rdbms-3.2.9.jar stream.py

spark-submit --master spark://ip-10-0-0-8:7077 --executor-memory 600M --total-executor-cores 2 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 partition_streaming.py #--jars /usr/lib/spark/lib/datanucleus-api-jdo-3.2.6.jar,/usr/lib/spark/lib/datanucleus-core-3.2.10.jar,/usr/lib/spark/lib/datanucleus-rdbms-3.2.9.jar stream.py

#spark-submit stream.py
