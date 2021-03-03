import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger

import java.util.Calendar

object StreamingStaticJoin extends App {

  // https://stackoverflow.com/questions/37638519/spark-streaming-how-to-periodically-refresh-cached-rdd
  val spark = SparkSession.builder()
    .appName("Kafka2Console")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val deltaPath = "file:///tmp/delta/table"

  var staticDf = spark.read.format("delta").load(deltaPath)
  staticDf.persist()

  def foreachBatchMethod[T](batchDf: Dataset[T], batchId: Long) = {
    staticDf.unpersist()
    staticDf = spark.read.format("delta").load(deltaPath)
    staticDf.persist()
    println(s"${Calendar.getInstance().getTime}: Refreshing static Dataframe from DeltaLake")
  }

  val staticRefreshStream = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .option("numPartitions", 1)
    .load()
    .selectExpr("CAST(value as LONG) as trigger")
    .as[Long]

  val streamingDf = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("CAST(value AS STRING) as id", "offset as streamingField")

  val joinDf = streamingDf.join(staticDf, "id")

  val query = joinDf.writeStream
    .format("console")
    .option("truncate", false)
    .option("checkpointLocation", "/home/michael/sparkCheckpoint")
    .start()

  staticRefreshStream.writeStream
    .outputMode("append")
    .foreachBatch(foreachBatchMethod[Long] _)
    .queryName("RefreshStream")
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .start()

  spark.streams.awaitAnyTermination()

}


