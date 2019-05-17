#!/bin/bash

spark-submit --master spark://ip-10-0-0-8:7077 --executor-memory 1G --total-executor-cores 12 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 --py-files ../cython/cy_utils.so stream.py 


