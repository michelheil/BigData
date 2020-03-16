import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.michael.big.data.spark.partitioning.{KeyPartitioner, Timer}

import scala.util.Random

object Main extends App {

  // create SparkSession
  val spark = SparkSession.builder()
    .appName("Partitioning")
    .master("local[3]")
    .getOrCreate()

  // set logLevel
  spark.sparkContext.setLogLevel("WARN")

  // create test data
  val endRange: Long = 10000000L
  val numbers: RDD[Long] = spark.range(0, endRange, 1, 3).toDF().rdd.map(row => row.getLong(0))

  // create keys
  val keyedNumbers: RDD[(Int, Long)] = numbers.keyBy(_ => 1 + Random.nextInt(3)).cache() // cache is important !!!
  // If 'cache' is not set the remaining results will not make sense as new random numbers will be generated on every(!) RDD action again.

  // Zero value for the aggregationByKey method
  val zeroValue: (Long, Long) = (0L, 0L)

  // Calculate result of Test A
  Timer.time {
    println("\nTest A:")
    val rdd1: RDD[(Int, (Long, Long))] = keyedNumbers.aggregateByKey[(Long, Long)](zeroValue)(seqOp, combOp)
    println("Print keyedNumbers of RDD1")
    rdd1.foreachPartition(it =>
      it.foreach(tuple => println(tuple)
      ))

    val finalAResults: Array[(Int, BigDecimal)] = rdd1.mapValues(v => myRounding((1.0 * v._1) / v._2)).collect()
    println("Average A per Key:")
    for (finalResult <- finalAResults) println(finalResult)
  }

  // Calculate result of Test B
  Timer.time {
    println("\nTest B:")
    val rdd2: RDD[(Int, (Long, Long))] = keyedNumbers.partitionBy(new KeyPartitioner).aggregateByKey[(Long, Long)](zeroValue)(seqOp, combOp)
    println("Print keyedNumbers of RDD2")
    rdd2.foreachPartition(it =>
      it.foreach(tuple => println(tuple)
      ))

    val finalBResults: Array[(Int, BigDecimal)] = rdd2.mapValues(v => myRounding((1.0 * v._1) / v._2)).collect()
    println("Sum per Key:")
    for (finalResult <- finalBResults) println(finalResult)
  }

  // Within-Partition Reduction Step
  def seqOp(a: (Long, Long), b: Long): (Long, Long) = (a._1 + b, a._2 + 1)

  // Cross-Partition Reduction Step
  def combOp(a: (Long, Long), b: (Long, Long)): (Long, Long) = (a._1 + b._1, a._2 + b._2)

  // Round to two decimals
  def myRounding(d: Double): BigDecimal = {
    BigDecimal(d).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }
}
