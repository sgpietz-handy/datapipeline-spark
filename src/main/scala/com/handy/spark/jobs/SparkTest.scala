package com.handy.spark.jobs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object SparkTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-test")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(1 to 10)
    rdd.collect().foreach(a => println(a))
  }
}
