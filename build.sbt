name := "handy-pipeline"

version := "0.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.5.2" % "provided",
  "org.apache.spark" % "spark-hive_2.10" % "1.5.2" % "provided",
  "com.databricks" % "spark-redshift_2.10" % "1.1.0",
  "com.github.scopt" %% "scopt" % "3.3.0",
  "io.circe" %% "circe-core" % "0.6.1",
  "io.circe" %% "circe-generic" % "0.6.1",
  "io.circe" %% "circe-parser" % "0.6.1",
  "org.typelevel" %% "cats" % "0.8.1"
)
