import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.TimestampType

object Kafka2Console_Update extends App {

  val spark = SparkSession.builder()
    .appName("Kafka2Console")
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._

  //spark.sparkContext.setLogLevel("DEBUG")
  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .option("maxOffsetsPerTrigger", "10")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("CAST(value AS STRING) as group", "timestamp")
    .select(
      col("group"),
      col("timestamp"),
      unix_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss").alias("time_unix"))

  val dfGrouped = df.groupBy(col("group"),
      window($"timestamp", "10 seconds")
    ).agg(max("time_unix").alias("max_time_unix"))
    .withColumn("time", col("max_time_unix").cast(TimestampType))
    .drop("window", "max_time_unix")

  val query: StreamingQuery = dfGrouped.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", "false")
    .option("checkpointLocation", "/home/michael/sparkCheckpoint7")
    .queryName("StackoverflowTest")
    .start()

  query.awaitTermination()

}


