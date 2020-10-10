import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.SparkSession

object Main extends App {

  val spark = SparkSession.builder()
    .appName("myAppName")
    .master("local[*]")
    .getOrCreate()

  // create DataFrame
  import spark.implicits._
  val df = Seq((3, "Alice"), (5, "Bob")).toDF("age", "name")
  df.show(false)

  // write to Kafka as is with "age" as key and "name" as value
  df.selectExpr("CAST(age AS STRING) as key", "CAST(name AS STRING) as value")
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "test-topic")
    .save()

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

}


