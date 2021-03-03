name := "Dataframe2Delta"

version := "0.1"

//scalaVersion := "2.11.12"
scalaVersion := "2.12.10"

resolvers += "MavenRepository" at "https://mvnrepository.com/"
//resolvers += "Hortonworks" at "https://repo.hortonworks.com/content/repositories/releases/"
resolvers += "Confluent" at "https://packages.confluent.io/maven/"

// Spark Information
val sparkVersion = "3.0.1"

//libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "io.delta" %% "delta-core" % "0.7.0"
//"io.confluent" % "kafka-schema-registry-client" % "5.4.1",
// "io.confluent" % "kafka-avro-serializer" % "5.4.1",
  //"org.apache.kafka" %% "kafka-clients" % "2.5.0",
// "org.apache.spark" %% "spark-avro" % sparkVersion
)

//libraryDependencies += "org.apache.kafka" %% "kafka-clients" % "2.4.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
