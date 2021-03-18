import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

object Kafka2File extends App {

  val spark = SparkSession.builder()
    .appName("Kafka2File")
    .master("local[*]")
    .getOrCreate()

  val fileOutputPathBefore = "file:///tmp/file/before"
  val fileOutputPathAfter = "file:///tmp/file/after"

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("CAST(key AS STRING) as key",
      "CAST(value AS STRING) as value", "topic", "partition", "offset", "timestamp")

  val query: StreamingQuery = df.writeStream
    .format("csv")
    .option("path", fileOutputPathAfter)
    .option("checkpointLocation", "/home/michael/sparkCheckpoint")
    .start()

  query.awaitTermination()

}