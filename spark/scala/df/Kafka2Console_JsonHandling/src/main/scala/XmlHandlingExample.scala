import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

object XmlHandlingExample extends App {

  val spark = SparkSession.builder()
    .appName("Kafka2Console")
    .master("local[*]")
    .getOrCreate()

  //spark.sparkContext.setLogLevel("DEBUG")
  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("CAST(value AS STRING) as payload")
    .selectExpr(
      "xpath(payload, '/CofiResults/ExecutionTime/text()') as ExecutionTimeAsArryString",
      """xpath_long(payload, '/CofiResults/ExecutionTime/text()') as ExecutionTimeAsLong""",
      """xpath_string(payload, '/CofiResults/ExecutionTime/text()') as ExecutionTimeAsString""",
      """xpath_int(payload, '/CofiResults/InputData/ns2/HeaderSegment/Version/text()') as VersionAsInt""")

// Example input data
// <CofiResults><ExecutionTime>20201103153839</ExecutionTime><FilterClass>S</FilterClass><InputData><ns2><HeaderSegment><Version>6</Version><SequenceNb>1</SequenceNb></HeaderSegment></ns2></InputData></CofiResults>

  val query: StreamingQuery = df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", "/home/michael/sparkCheckpoint/xml")
    .start()

  query.awaitTermination()

}


