#!/bin/bash

spark-submit --master spark://ip-10-0-0-6:7077 --executor-memory 2G --driver-memory 6G rparquet.py 

