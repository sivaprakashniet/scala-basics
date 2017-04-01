name := """simple-jobs"""

version := "1.0-SNAPSHOT"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "org.apache.spark" %% "spark-mllib" % "2.0.1",
  "org.apache.hadoop" % "hadoop-client" % "2.7.0",
  "org.apache.spark" %% "spark-mllib-local" % "2.0.1"
)