#!/bin/bash

spark-submit --master spark://ip-10-0-0-4:7077 --executor-memory 10G --driver-memory 10G pipe_model.py 

