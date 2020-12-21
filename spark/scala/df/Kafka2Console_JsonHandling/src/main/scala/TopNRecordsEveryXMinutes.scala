import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery

object TopNRecordsEveryXMinutes extends App {

  val spark = SparkSession.builder()
    .appName("Kafka2Console_TopN")
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
    .selectExpr("CAST(value AS STRING) as value", "timestamp")

  val aggDF = df
    .withWatermark("timestamp", "0 seconds")
    .groupBy(col("value"), window(col("timestamp"), "30 seconds", "5 seconds"))
    .agg(count("value").as("counter"))
    //.orderBy(desc("counter"))

  val query: StreamingQuery = aggDF.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", "/home/michael/sparkCheckpoint")
    //.trigger(Trigger.ProcessingTime(5000))
    .start()

  query.awaitTermination()

}


