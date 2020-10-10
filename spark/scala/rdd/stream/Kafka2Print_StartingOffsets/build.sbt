name := "sparkDirectKafkaOffsetRanges"

version := "0.1"

scalaVersion := "2.11.12"


resolvers += "MavenRepository" at "https://mvnrepository.com/"

// Spark Information
val sparkVersion = "2.3.2"

libraryDependencies ++= Seq(
  // spark core
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
)