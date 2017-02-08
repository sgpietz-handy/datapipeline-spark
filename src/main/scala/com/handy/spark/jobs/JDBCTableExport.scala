package com.handy.spark.jobs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, DataFrame, SaveMode}
import org.apache.spark.sql.types.{MetadataBuilder, StringType, StructField}

import cats.implicits._
import io.circe.{JsonObject}
import io.circe.parser.parse
import io.circe.syntax._
import scopt.OptionParser


object JDBCTableExport {
  case class Config(
    url: String = "",
    sourceTable: String = "",
    targetTable: String = ""
  )

  object Config {
    val parser =
      new OptionParser[Config]("spark-submit spark-jobs-<version>.jar") {
        arg[String]("url") action { (x, c) =>
          c.copy(url = x)
        } text ("redshift jdbc url")
        arg[String]("source-table") action { (x, c) =>
          c.copy(sourceTable = x)
        } text ("source table name")
        arg[String]("target-table") action { (x, c) =>
          c.copy(targetTable = x)
        } text ("target table name")
      }
    }

  def main(args: Array[String]): Unit = {
    println(s"driver class: ${Class.forName("com.mysql.jdbc.Driver")}")
    val Some(Config(url, sourceTable, targetTable)) =
      Config.parser.parse(args, Config())

    val conf = new SparkConf().setAppName("jdbc-table-export")
    implicit val sc = new SparkContext(conf)
    implicit val sqlContext = new HiveContext(sc)

    val cl = ClassLoader.getSystemClassLoader

    cl.asInstanceOf[java.net.URLClassLoader].getURLs.foreach(println)
    println("driver classpath:")
    println(conf.get("spark.driver.extraClassPath"))
    println("executor classpath:")
    println(conf.get("spark.executor.extraClassPath"))

    val df = sqlContext.read.format("jdbc").options(
      Map("driver"  -> "com.mysql.jdbc.Driver",
          "url"     -> url,
          "dbtable" -> sourceTable)).load()

    df.write.format("orc").mode(SaveMode.Overwrite).saveAsTable(targetTable)
  }
}
