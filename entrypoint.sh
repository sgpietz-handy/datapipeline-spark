#!/bin/bash

spark-submit --master yarn-cluster --class com.handy.spark.jobs.$1 /handy-pipeline-assembly.jar "${@:2}"
