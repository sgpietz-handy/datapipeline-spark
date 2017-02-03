#!/bin/bash

spark-submit --master yarn-cluster --jars /RedshiftJDBC.jar --class com.handy.spark.jobs.$1 /handy-pipeline-assembly.jar "${@:2}"
