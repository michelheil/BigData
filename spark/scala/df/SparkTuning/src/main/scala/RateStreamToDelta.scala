import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StringType

import java.util.Calendar
import scala.sys.process.Process

object RateStreamToDelta extends App {

  val deltaPath = "file:///tmp/delta/table"
  val checkpointLocation = "/tmp/spark/checkpointLocation"

  // clean up checkpoint location
  val cleanUpCommand = "rm -rf " + checkpointLocation
  Process(cleanUpCommand).!

  def getRandomBalance(): String = scala.util.Random.nextInt(20000).toString
  def getRandomMiete(): String = (-1 * scala.util.Random.nextInt(1337)).toString

  val spark = SparkSession.builder()
    .appName("DF2Delta")
    .master("local[*]")
    .getOrCreate()

  val getRandomBalanceUDF = udf(getRandomBalance _)
  val getRandomMieteUDF = udf(getRandomMiete _)

  val bronzeRateStreamDf = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .option("numPartitions", 1)
    .load()

  val silverRateStreamDf = bronzeRateStreamDf
    .withColumn("balance", getRandomBalanceUDF())
    .withColumn("miete", getRandomMieteUDF())
    .withColumn("datum", to_date(col("timestamp")).cast(StringType))

  val queryDelta = silverRateStreamDf.writeStream
      .format("delta")
      .outputMode("append")
      .queryName("RateToDelta")
      .option("checkpointLocation", checkpointLocation)
      .start(deltaPath)

  new Thread(new StreamingMonitor(queryDelta)).start()

  queryDelta.awaitTermination()

  class StreamingMonitor(q: StreamingQuery) extends Runnable {
    def run {
      while (true) {
        Thread.sleep(10000)
        println("Time: " + Calendar.getInstance().getTime())
        println(q.lastProgress.prettyJson)
      }
    }
  }

}