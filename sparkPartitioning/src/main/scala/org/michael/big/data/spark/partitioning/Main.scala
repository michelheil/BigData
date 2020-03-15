import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Main extends App {

  // create SparkSession
  val spark = SparkSession.builder()
    .appName("Partitioning")
    .master("local[3]")
    .getOrCreate()

  // set logLevel
  spark.sparkContext.setLogLevel("WARN")

  // create Test data
  val myCollection: Array[String] = "Hallo! dies ist ein Satz zum testen von Spark Partitionierungen".split(" ")
  val words: RDD[String] = spark.sparkContext.parallelize(myCollection, 3)

  val biggerTestData: RDD[String] = spark.sparkContext.textFile("src/main/resources/log4j.properties")

  // use first latter as key for the RDD. The first letter is set to lower case.
  val biggerTestDataWords = biggerTestData.keyBy(word => word.toLowerCase.head.toString)
  for(word <- biggerTestData) println(word)

  val startTimer: Long = System.currentTimeMillis()
}
