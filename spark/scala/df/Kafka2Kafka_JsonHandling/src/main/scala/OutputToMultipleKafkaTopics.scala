import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.functions.when

object OutputToMultipleKafkaTopics extends App {

  val spark = SparkSession.builder()
    .appName("Single2MultipleTopics")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val filter1 = col("value") === 1
  val filter2 = col("key") === 1
  val filterList = List(filter1, filter2)
  val filter = filterList.tail.foldLeft(filterList.head)((f1, f2) => f1.and(f2))
  println(filter)

  val inputDf = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .option("failOnDataLoss", "false") // in case offsets or entire topic are getting deleted
    .load()
    .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value", "partition", "offset", "timestamp")

  val filteredDf = inputDf.withColumn("topic", when(filter, lit("out1")).otherwise(lit("out2")))

  val query = filteredDf
    .select(
      col("key"),
      to_json(struct(col("*"))).alias("value"),
      col("topic"))
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("checkpointLocation", "/home/michael/sparkCheckpoint/1/")
    .start()

  query.awaitTermination()
}


