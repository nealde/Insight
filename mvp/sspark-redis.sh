#!/bin/bash

#spark-submit --master spark://ip-10-0-0-4:7077 --executor-memory 4G --driver-memory 6G rparquet.py 
# --packages com.redislabs:spark-redis:2.3.0
spark-submit --jars sr.jar --conf "spark.redis.host=10.0.0.10" --conf "spark.redis.port=6379" spark-redis.py
