import org.apache.spark.sql.SparkSession

object OutputToTwoKafkaTopics extends App {

  val spark = SparkSession.builder()
    .appName("myAppName")
    .master("local[*]")
    .getOrCreate()

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .option("failOnDataLoss", "false") // in case offsets or entire topic are getting deleted
    .load()

  // It is important to
  // - use two different checkpoint locations
  // - call `awaitTermination` after calling `start` of the second query
  val trump_topic = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "trump")
    .option("checkpointLocation", "/home/michael/sparkCheckpoint/1/")
    .start()

  val biden_topic = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "biden")
    .option("checkpointLocation", "/home/michael/sparkCheckpoint/2/")
    .start()

  spark.streams.awaitAnyTermination()
}


