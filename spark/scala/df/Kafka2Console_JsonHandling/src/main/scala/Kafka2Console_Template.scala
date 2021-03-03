import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener}

object Kafka2Console_Template extends App {

  val spark = SparkSession.builder()
    .appName("Kafka2Console")
    .master("local[*]")
    .getOrCreate()

  //spark.sparkContext.setLogLevel("DEBUG")
  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .option("maxOffsetsPerTrigger", "10")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("CAST(value AS STRING) as value", "timestamp")

  spark.streams.addListener(new StreamingQueryListener {
    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

    override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
      println("Value of event.progress.NumInputRows: " + event.progress.numInputRows)
    }

    override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
  })

  val query: StreamingQuery = df.writeStream
    //.format("console")
    .outputMode("append")
    .foreachBatch((ds: Dataset[Row], batchId: Long) => {
      println(ds.count())
      ds.write.format("csv").mode(SaveMode.Append).save("/tmp/test/tmp.csv")
    })
    //.option("truncate", "false")
    //.option("checkpointLocation", "/home/michael/sparkCheckpoint")
    .queryName("StackoverflowTest")
    //.trigger(Trigger.ProcessingTime(1000))
    .start()

  query.awaitTermination()

}


