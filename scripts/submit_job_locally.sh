#!/bin/bash
mode="${1:-local}"
source_file="${2:-Motor_Vehicle_Collisions_-_Crashes_HEAD.csv}"

spark-submit --name PySpark_Practice /home/ilmancino/PycharmProjects/spark-pyspark-practice/python/main.py $mode "$source_file"
