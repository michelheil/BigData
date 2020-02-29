name := "sparkHBaseConnector"

version := "0.1"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"

// https://mvnrepository.com/artifact/org.apache.hbase.connectors.spark/hbase-spark
libraryDependencies += "org.apache.hbase.connectors.spark" % "hbase-spark" % "1.0.0"

// https://mvnrepository.com/artifact/com.hortonworks.shc/shc-core
libraryDependencies += "com.hortonworks.shc" % "shc-core" % "1.1.0.3.1.0.0-78"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1"