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
      (k, v.withObject(o => o.asJson.noSpaces.asJson))
    }.toMap.asJson.asObject.get

  def getDF(s: Source)(implicit sc: SparkContext, sqlContext: SQLContext)
  : DataFrame = s match {
    case Table(name) => sqlContext.table(name)
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
      } text ("redshift table name")
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

  def main(args: Array[String]): Unit = {
    val Some(Config(redshiftTable, url, tempdir, saveMode, source)) =
      Config.parser.parse(args, Config())

    val conf = new SparkConf().setAppName("redshift-json-writer")
    implicit val sc = new SparkContext(conf)
    implicit val sqlContext = new HiveContext(sc)

    def withLegalName(df: DataFrame, name: String): DataFrame = {
      def stripIllegal(s: String) = s.replaceAll("[^a-zA-Z]", "")
      df.withColumnRenamed(name, stripIllegal(name))
    }

    val table: DataFrame = source.map(s =>
      Source.getDF(s)
    ).getOrElse(throw new Exception("must specify one of -t, -f, or -e"))

    val df = table.schema.fields.foldLeft(table) {
      case (t, StructField(name, StringType, _, _)) => {
        val metadata = new MetadataBuilder().putString("redshift_type", "VARCHAR(MAX)").build()
        withLegalName(t.withColumn(name, t(name).as(name, metadata)), name)
      }
      case (t, StructField(name, _, _, _)) => withLegalName(t, name)
    }

    df.write
      .format("com.databricks.spark.redshift")
      .option("url", url)
      .option("dbtable", redshiftTable)
      .option("tempdir", tempdir)
      .mode(saveMode)
      .save()
  }
}
