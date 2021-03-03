name := "sparkStackoverflow"

version := "0.1"

scalaVersion := "2.11.12"
//scalaVersion := "2.12.10"

resolvers += "MavenRepository" at "https://mvnrepository.com/"
//resolvers += "Hortonworks" at "https://repo.hortonworks.com/content/repositories/releases/"
resolvers += "Confluent" at "https://packages.confluent.io/maven/"

// Spark Information
val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
//  "org.apache.spark" %% "spark-streaming" % sparkVersion,
 "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
  //"org.apache.kafka" %% "kafka-clients" % "2.5.0",
// "org.apache.spark" %% "spark-avro" % sparkVersion
)
