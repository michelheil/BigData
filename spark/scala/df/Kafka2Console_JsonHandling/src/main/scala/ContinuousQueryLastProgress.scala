import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

object ContinuousQueryLastProgress extends App {

  val spark = SparkSession.builder()
    .appName("Kafka2Console")
    .master("local[*]")
    .getOrCreate()

  // spark.sparkContext.setLogLevel("DEBUG")
  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .option("maxOffsetsPerTrigger", "1")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value", "timestamp")

  val query: StreamingQuery = df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    //.option("checkpointLocation", "/home/michael/sparkCheckpoint")
    .start()

  println(query.sparkSession.sparkContext.getConf.getAll.toString)
  println(query.sparkSession.sparkContext.getCheckpointDir)
  new Thread(new StreamingMonitor(query)).start()

  query.awaitTermination()

  class StreamingMonitor(q: StreamingQuery) extends Runnable {
    def run {
      while(true) {
        println("Time: " + Calendar.getInstance().getTime())
        println(q.lastProgress)
        Thread.sleep(10000)
      }
    }
  }
}


