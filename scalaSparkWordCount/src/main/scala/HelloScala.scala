import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// https://hortonworks.com/tutorial/setting-up-a-spark-development-environment-with-scala/

object HelloScala {

  def main(args: Array[String]) {

    // Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val textFile = sc.textFile("/home/michael/spark/current/README.md") //HDP Sandbox "hdfs:///tmp/shakespeare.txt"

    // word count
    val counts: RDD[(String, Int)] = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    System.out.println("Total words: " + counts.count())

    counts.saveAsTextFile("/home/michael/git-repositories/scalaBasic/src/test/resources/readmeWordCount.txt") // HDP Sandbox: "hdfs:///tmp/shakespeareWordCount"
  }

}

// Deploy job on HDP-Sandbox
// cd ~/git-repository/scalaBasic
// sbt package
// cd target/scala-2.11/
// scp -P 22 scalabasic_2.11-0.1.jar root@172.18.0.2:/root (Hierzu muss vorher das Passwort geaendert werden)
// spark-submit --class HelloScala  --master yarn ./helloscala_2.11-0.1.jar
