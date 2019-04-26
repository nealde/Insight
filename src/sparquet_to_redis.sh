#!/bin/bash

spark-submit --master spark://ip-10-0-0-8:7077 --executor-memory 10G --driver-memory 6G parquet_to_redis.py 
