name := "StructuredStreamingML"
version := "0.1"
scalaVersion := "2.12.10"

resolvers += "MavenRepository" at "https://mvnrepository.com/"
resolvers += "Confluent" at "https://packages.confluent.io/maven/"

// Spark Information
val sparkVersion = "3.1.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "io.delta" %% "delta-core" % "0.7.0"
)