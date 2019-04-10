#!/bin/bash

#spark-submit --master spark://ip-10-0-0-6:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 --jars /usr/lib/spark/lib/datanucleus-api-jdo-3.2.6.jar,/usr/lib/spark/lib/datanucleus-core-3.2.10.jar,/usr/lib/spark/lib/datanucleus-rdbms-3.2.9.jar stream.py

spark-submit --master spark://ip-10-0-0-4:7077 --executor-memory 8G --total-executor-cores 12 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 --py-files cy_utils.so streaming.py --files cy_utils.so #--jars /usr/lib/spark/lib/datanucleus-api-jdo-3.2.6.jar,/usr/lib/spark/lib/datanucleus-core-3.2.10.jar,/usr/lib/spark/lib/datanucleus-rdbms-3.2.9.jar stream.py

#spark-submit stream.py
