#!/bin/bash

master=$1
class="com.handy.spark.jobs.$2"
args=${@:3}

echo "master = $master"
echo "class = $class"
echo "args = $args"

spark-submit --master $master --files /usr/hdp/current/spark-client/conf/hive-site.xml --class $class /handy-pipeline-assembly.jar $args

# spark-submit --master $master --files /usr/hdp/current/spark-client/conf/hive-site.xml,/usr/hdp/current/spark-client/conf/spark-defaults.conf --class $class /handy-pipeline-assembly.jar $args
