#!/bin/bash

master=$1
class="com.handy.spark.jobs.$2"
args=${@:3}

echo "master = $master"
echo "class = $class"
echo "args = $args"
spark-submit --master $master --jars /RedshiftJDBC.jar --class $class /handy-pipeline-assembly.jar $args
