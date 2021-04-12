name := "Kafka2Print_StartingOffsets"

version := "0.1"

scalaVersion := "2.12.11"


resolvers += "MavenRepository" at "https://mvnrepository.com/"

// Spark Information
val sparkVersion = "3.1.1"

libraryDependencies ++= Seq(
  // spark core
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
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