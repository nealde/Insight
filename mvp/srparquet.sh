#!/bin/bash

spark-submit --master spark://ip-10-0-0-4:7077 --executor-memory 4G --driver-memory 6G rparquet.py 

