#!/bin/bash

spark-submit --master spark://ip-10-0-0-4:7077 --executor-memory 7G --driver-memory 7G batch_process.py 
