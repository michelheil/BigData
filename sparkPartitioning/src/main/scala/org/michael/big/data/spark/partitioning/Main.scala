import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
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
  val endRange: Long = 10L
  val numbers: RDD[Long] = spark.range(1, endRange, 1, 3).toDF().rdd.map(row => row.getLong(0))

  // create keys
  val keyedNumbers: RDD[(Int, Long)] = numbers.keyBy(_ => 1 + Random.nextInt(3))
  println("Test data:")
  for(number <- keyedNumbers) println(number)

  val startTimer: Long = System.currentTimeMillis()

  // Calculate test a result
  val zeroValue: (Long, Long) = (0L, 0L)
  val rdd1 = keyedNumbers.aggregateByKey[(Long, Long)](zeroValue)(seqOp, combOp)

  val finalResults = rdd1.mapValues(v => v._1/v._2).collect()
  println("Average per Key:")
  for(finalResult <- finalResults) println(finalResult)
  /*
  >>> aTuple = (0,0) # As of Python3, you can't pass a literal sequence to a function.
  >>> rdd1 = rdd1.aggregateByKey(aTuple, lambda a,b: (a[0] + b,    a[1] + 1),
    lambda a,b: (a[0] + b[0], a[1] + b[1]))

  >>> finalResult = rdd1.mapValues(lambda v: v[0]/v[1]).collect()
  >>> print(finalResult)
  */

  // Within-Partition Reduction Step
  def seqOp(a: (Long, Long), b: Long): (Long, Long) = (a._1 + b, a._2 + 1)

  // Cross-Partition Reduction Step
  def combOp(a: (Long, Long), b: (Long, Long)): (Long, Long) = (a._1 + b._1, a._2 + b._2)

}

/*
* Create an RDD with test data distributed over 3 partitions
  * Assign a key out of 1, 2, or 3 to each value independent of their partition
  * Test A: Process the values per key
* Test B: Repartition the RDD based on the key and perform identical process
* Measure time for Test A and Test B and compare the results
 */