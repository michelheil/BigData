import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds

object filterPortInput {

  def main(args: Array[String]) {

    // Create conf for SparkStreamingContext
    val conf = new SparkConf().
      setAppName("filterPortInput").setMaster("local[*]")

    val sc = new SparkContext(conf)

    // Create a StreamingContext with a 1-second batch size from a SparkConf
    val ssc = new StreamingContext(sc, Seconds(10))

    // Create a DStream using data received after connecting to port 7777 on the local machine
    val dstream = ssc.socketTextStream("localhost", 7777)

    // Filter our DStream for lines with error "error"
    val errorLines = dstream.filter(_.contains("error"))

    // Print out the lines with errors
    errorLines.print()


    // Start our streaming context and wait for it to finish
    ssc.start()
    // Wait for the job to finish
    ssc.awaitTermination()

  }

}
