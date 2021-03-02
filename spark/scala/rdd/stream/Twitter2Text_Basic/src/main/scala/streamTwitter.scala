/*
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.TwitterStream
import twitter4j.TwitterStreamFactory

object streamTwitter {

  def main(args: Array[String]) {

    // Validate input parameters
    if (args.length < 4) {
      System.err.println("Usage: TwitterData <ConsumerKey><ConsumerSecret><accessToken><accessTokenSecret>" + "[<filters>]")
      System.exit(1)
    }

    // store input parameters in a collection
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // set system properties so that Twitter4J library can use it
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Erstelle Spark Conf
    val conf = new SparkConf().setAppName("streamTwitter").setMaster("local")

    // Erstelle Streaming Context
    val ssc = new StreamingContext(conf, Seconds(5))

    val stream = TwitterUtils.createStream(ssc, None, filters)

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    hashTags.saveAsTextFiles("tweets", "json")

    ssc.start()
    ssc.awaitTermination()
  }

}
*/
// Deploy job on HDP-Sandbox
// cd ~git-repository/sparkStreaming/
// mvn clean install
// cd target/
// scp -P 22 sparkStreaming*.jar root@172.18.0.2:/root (Hierzu muss vorher das Passwort geaendert werden)
// In der Web Shell von der Sandbox:
// In das Home Verzeichnis von user 'root' wechseln
// spark-submit --class streamTwitter --master yarn ./sparkStreaming*.jar
