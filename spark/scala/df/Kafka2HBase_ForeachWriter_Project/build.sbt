name := "sparkMonitoring"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += "MavenRepository" at "https://mvnrepository.com/"
resolvers += "Hortonworks" at "https://repo.hortonworks.com/content/repositories/releases/"

// Spark Information
val sparkVersion = "2.4.5"
val hbaseVersion = "2.1.5"

libraryDependencies ++= Seq(
  // spark core
  "org.apache.spark" %% "spark-core" % sparkVersion exclude("org.apache.hadoop", "hadoop-client") exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "org.apache.spark" %% "spark-sql" % sparkVersion exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
)

// hbase
libraryDependencies += "org.apache.hbase" % "hbase-client" % hbaseVersion exclude("com.fasterxml.jackson.core", "jackson-databind")
libraryDependencies += "org.apache.hbase" % "hbase-common" % hbaseVersion exclude("com.fasterxml.jackson.core", "jackson-databind")

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-hdfs
//libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.8.5"

// https://mvnrepository.com/artifact/com.typesafe/config
libraryDependencies += "com.typesafe" % "config" % "1.3.4"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % Test

// https://mvnrepository.com/artifact/com.hortonworks.shc/shc-core
//libraryDependencies += "com.hortonworks.shc" % "shc-core" % "1.1.0.3.1.5.6-1"

