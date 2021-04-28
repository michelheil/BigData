import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

object DataFrameToDelta extends App {

  // https://stackoverflow.com/questions/37638519/spark-streaming-how-to-periodically-refresh-cached-rdd
  val deltaPath = "file:///tmp/delta/table"

  val spark = SparkSession.builder()
    .appName("DF2Delta")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val df = Seq(
    (1L, "static1"),
    (2L, "static2"),
    (3L, "static3")
  ).toDF("id", "deltaField")

  df.show(false)

  df.write
    .mode(SaveMode.Overwrite)
    .format("delta")
    .save(deltaPath)

  spark.read
    .format("delta")
    .load(deltaPath)
    .createOrReplaceTempView("testTable")

  spark.sql("SELECT * FROM testTable").show(false)

}