import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructType}

object JsonHandling extends App {

  val spark = SparkSession.builder()
    .appName("myAppName")
    .master("local[*]")
    .getOrCreate()

  val schema: StructType = new StructType()
    .add("id", StringType)
    .add("time_series", ArrayType(new StructType()
      .add("time", StringType)
      .add("value", DoubleType)
    ))

  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "json")
    .option("startingOffsets", "earliest")
    .load()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val df1 = df
    .selectExpr("CAST(value as STRING) as json")
    .select(from_json('json, schema).as("data"))
    .select(col("data.id").as("id"), explode(col("data.time_series")).as("time_series"))
    .select(col("id"), col("time_series.time").as("time"), col("time_series.value").as("value"))

  df1.printSchema()

  df1.writeStream
    .format("console")
    .option("truncate", "false")
    .start()
    .awaitTermination()

}


