import org.apache.spark.sql.SparkSession

object Main extends App {

  case class KafkaRecord(partition: Int, value: String)

  val spark = SparkSession.builder()
    .appName("myAppName")
    .master("local[*]")
    .getOrCreate()



  // create DataFrame
  import spark.implicits._
  val df = Seq((0, "Alice"), (1, "Bob")).toDF("partition", "value").as[KafkaRecord].drop("partition")
  df.show(false)

  // write to Kafka as is with "age" as key and "name" as value
  df//.selectExpr("CAST(partition AS STRING) as partition", "CAST(value AS STRING) as value")
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "test")
    .option("kafka.partitioner.class", "org.test.CustomPartitioner")
    .save()
/*
  // convert columns into json string
  val df2 = df.select(col("name"),to_json(struct($"*"))).toDF("key", "value")
  df2.show(false)

  // write to Kafka with jsonString as value
  df2.selectExpr("key", "value")
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "test-topic")
    .save()
*/
}


