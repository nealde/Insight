#!/bin/bash

spark-submit --master spark://ip-10-0-0-6:7077 --executor-memory 4G --driver-memory 4G train_model.py 

