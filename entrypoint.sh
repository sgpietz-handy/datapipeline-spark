#!/bin/bash

master=$1
class="com.handy.spark.jobs.$2"
args=${@:3}

echo "master = $master"
echo "class = $class"
echo "args = $args"
spark-submit --master $master --jars /RedshiftJDBC.jar,/usr/hdp/current/spark-client/lib/datanucleus-api-jdo-3.2.6.jar,/usr/hdp/current/spark-client/lib/datanucleus-core-3.2.10.jar,/usr/hdp/current/spark-client/lib/datanucleus-rdbms-3.2.9.jar --files /usr/hdp/current/spark-client/conf/hive-site.xml --class $class /handy-pipeline-assembly.jar $args
