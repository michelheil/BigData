import org.apache.spark.sql.SparkSession

object Kafka2Delta extends App {

  val spark = SparkSession.builder()
    .appName("Kafka2Console")
    .master("local[*]")
    .getOrCreate()

  val deltaPath = "file:///tmp/delta/table"
/*
  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("CAST(value AS STRING) as value")

  val query: StreamingQuery = df.writeStream
    .format("delta")
    .option("checkpointLocation", "/home/michael/sparkCheckpoint")
    .start(deltaPath)

  query.awaitTermination()
  */
  val table = spark.read
    .format("delta")
    .load(deltaPath)
    .createOrReplaceTempView("testTable")

  spark.sql("SELECT * FROM testTable").show(false)

}


