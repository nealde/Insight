#!/bin/bash

spark-submit --master spark://ip-10-0-0-4:7077 --executor-memory 6G --driver-memory 3G redis_pipe.py 

