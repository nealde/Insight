#!/bin/bash

spark-submit --jars sr.jar --conf "spark.redis.host=10.0.0.10" --conf "spark.redis.port=6379" --master spark://ip-10-0-0-4:7077 --executor-memory 10G --driver-memory 6G parquet_query.py 
