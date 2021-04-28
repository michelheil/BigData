name := "Kafka2Kafka_JsonHandling"
scalaVersion := "2.12.13"

resolvers += "MavenRepository" at "https://mvnrepository.com/"

// Spark Information
val sparkVersion = "3.1.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "io.delta" %% "delta-core" % "0.8.0"
)

// *******************
// ScalaStyle settings
// *******************
scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"
lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := scalastyle.in(Compile).toTask("").value
(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value
lazy val testScalastyle = taskKey[Unit]("testScalastyle")
testScalastyle := scalastyle.in(Test).toTask("").value