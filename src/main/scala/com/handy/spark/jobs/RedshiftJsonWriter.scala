package com.handy.spark.jobs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.types.{MetadataBuilder, StringType, StructField}

import cats.implicits._
import io.circe.{JsonObject}
import io.circe.parser.parse
import io.circe.syntax._
import scopt.OptionParser

sealed trait Source
case class Table(name: String) extends Source
case class Query(sql: String) extends Source
case class JsonFile(url: String) extends Source

object Source {
  def objsToStr(obj: JsonObject): JsonObject =
    obj.toList.map { case (k, v) =>
      val jval = v.withObject(o => o.asJson.noSpaces.asJson)
        .withArray(a => a.asJson.noSpaces.asJson)
      (k, jval.withString(s =>
          (if (s.size > (65535 / 4)) s.take(65535 / 4) else s).asJson))
    }.toMap.asJson.asObject.get

  def getDF(s: Source)(implicit sc: SparkContext, sqlContext: SQLContext)
  : DataFrame = s match {
    case Table(name) => {
      println(s"copying table $name")
      sqlContext.table(name)
    }
    case Query(sql) => sqlContext.sql(sql)
    case JsonFile(url) => {
      val rdd = sc.textFile(url).map { a =>
        val b = for {
          json <- parse(a).toOption
          obj <- json.asObject
        } yield objsToStr(obj)

        b.get.asJson.noSpaces
      }
      sqlContext.jsonRDD(rdd)
    }
  }
}

case class Config(
  redshiftTable: String = "",
  url: String = "",
  accessKey: String = "",
  secretKey: String = "",
  tempdir: String = "",
  saveMode: String = "",
  source: Option[Source] = None
)

object Config {
  val parser =
    new OptionParser[Config]("spark-submit redshift-json-writer<version>.jar") {
      arg[String]("redshift-table") action { (x, c) =>
        c.copy(redshiftTable = x)
      } text ("redshift table name")
      arg[String]("url") action { (x, c) =>
        c.copy(url = x)
      } text ("redshift jdbc url")
      arg[String]("access-key") action { (x, c) =>
        c.copy(accessKey = x)
      } text ("aws access key id")
      arg[String]("secret-key") action { (x, c) =>
        c.copy(secretKey = x)
      } text ("aws secret access key")
      arg[String]("tempdir") action { (x, c) =>
        c.copy(tempdir = x)
      } text ("s3 temp directory to stage intermediate data files")
      arg[String]("saveMode") action { (x, c) =>
        c.copy(saveMode = x)
      } text ("spark write saveMode")
      opt[String]('t', "table") action { (x, c) =>
        c.copy(source = Some(Table(x)))
      } text ("hive table to import")
      opt[String]('f', "file") action { (x, c) =>
        c.copy(source = Some(JsonFile(x)))
      } text ("json file")
      opt[String]('e', "query-string") action { (x, c) =>
        c.copy(source = Some(Query(x)))
      } text ("query string for derived table")
    }
}


object RedshiftJsonWriter {

  def numCharToChar(a: Char): Char = a match {
    case '0' => 'a'
    case '1' => 'b'
    case '2' => 'c'
    case '3' => 'd'
    case '4' => 'e'
    case '5' => 'f'
    case '6' => 'g'
    case '7' => 'h'
    case '8' => 'i'
    case '9' => 'j'
    case _ => a
  }

  def legalizeName(name: String): String = {
    val alphanum = (('a' to 'z') ++ ('A' to 'Z') ++ ('1' to '9')).toSet
    numCharToChar(name.head) +:
      name.tail.map(a => if (alphanum.contains(a)) a else a.toInt.toChar)
  }

  def main(args: Array[String]): Unit = {
    val Some(Config(redshiftTable, url, accessKey, secretKey, tempdir, saveMode, source)) =
      Config.parser.parse(args, Config())

    val conf = new SparkConf().setAppName("redshift-json-writer")
    implicit val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", accessKey)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secretKey)

    implicit val sqlContext = new HiveContext(sc)

    def withLegalName(df: DataFrame, name: String): DataFrame = {
      def stripIllegal(s: String) = s.replaceAll("[^a-zA-Z]", "")
      df.withColumnRenamed(name, legalizeName(name))
    }

    val table: DataFrame = source.map(s =>
      Source.getDF(s)
    ).getOrElse(throw new Exception("must specify one of -t, -f, or -e"))

    val cl = ClassLoader.getSystemClassLoader

    cl.asInstanceOf[java.net.URLClassLoader].getURLs.foreach(println)
    println("driver:")
    println(conf.get("spark.driver.extraClassPath"))
    println("executor:")
    println(conf.get("spark.executor.extraClassPath"))

    val df = table.schema.fields.foldLeft(table) {
      case (t, StructField(name, StringType, _, _)) => {
        val metadata = new MetadataBuilder()
          .putString("redshift_type", "VARCHAR(MAX)")
          .build()

        val legalName = legalizeName(name)
        t.withColumnRenamed(name, legalName)
        t.withColumn(legalName, t(legalName).as(legalName, metadata))
      }
      case (t, sf) => t.withColumnRenamed(sf.name, legalizeName(sf.name))
    }

    df.write
      .format("com.databricks.spark.redshift")
      .option("url", url)
      .option("dbtable", redshiftTable)
      .option("tempdir", tempdir)
      .option("tempformat", "CSV")
      .mode(saveMode)
      .save()
  }
}
