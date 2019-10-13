import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.mqtt.MQTTUtils


object Main extends App {
  val sparkConf = new SparkConf(true).setAppName("MqttWordCount").setMaster("local[*]")
  val ssc = new StreamingContext(sparkConf, Seconds(10))

  val brokerURL = "tcp://localhost:1883"
  val topic = "/arbeitszimmer/temperatur"

  val lines = MQTTUtils.createStream(ssc, brokerURL, topic)

  val words = lines.flatMap(line => line.split(" "))
  val wordCount = words.map(word => (word, 1)).reduceByKey(_ + _)

  wordCount.print()

  ssc.start()
  ssc.awaitTermination()
}
