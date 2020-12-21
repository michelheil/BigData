import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

object SelfUpdatingKafkaSource extends App {

  val spark = SparkSession.builder()
    .appName("Kafka2Console")
    .master("local[*]")
    .getOrCreate()

  //spark.sparkContext.setLogLevel("DEBUG")
  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribePattern", "test.*")
    .option("maxOffsetsPerTrigger", "10")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("kafka.metadata.max.age.ms", "1000")  // every 1 second the subscribe pattern is used again to fetch all TopicPartitions for subscription
    .load()
    .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value", "timestamp")

  val query: StreamingQuery = df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", "/home/michael/sparkCheckpoint")
    .trigger(Trigger.ProcessingTime(1000))
    .start()

  query.awaitTermination()

}


