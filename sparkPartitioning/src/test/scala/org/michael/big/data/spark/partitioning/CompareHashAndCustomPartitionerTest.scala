package org.michael.big.data.spark.partitioning

import org.apache.spark.{HashPartitioner, Partitioner, TaskContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSpec, MustMatchers}

class CompareHashAndCustomPartitionerTest extends FunSpec with MustMatchers {

  val testSpark = SparkSession.builder()
    .appName("Partitioning")
    .master("local[*]")
    .getOrCreate()

  val inputData = Seq(
    ("EU", "testdaten1"),
    ("EU", "testdaten2"),
    ("EU", "testdaten3"),
    ("EU", "testdaten4"),
    ("US", "testdaten5"),
    ("US", "testdaten6")

  )

  // create RDD
  val rdd = testSpark.sparkContext.parallelize(inputData)
  println(s"Number of partitions of rdd: ${rdd.getNumPartitions}.")

  // Option 1 - Hash Partitioner
  val rddHash = rdd.partitionBy(new HashPartitioner(2))
  println(s"Number of partitions of rddHash: ${rddHash.getNumPartitions}.")
  rddHash.foreachPartition(iter => {
    iter.foreach(k => println(s"${TaskContext.getPartitionId()} ${k._1} ${k._2}"))
  })

  // Option 2 - Custom Partitioner
  val rddCustom = rdd.partitionBy(new TablePartitioner)
  println(s"Number of partitions of rddCustom: ${rddCustom.getNumPartitions}.")
  rddCustom.foreachPartition(iter => {
    iter.foreach(k => println(s"${TaskContext.getPartitionId()} ${k._1} ${k._2}"))
  })

}

// define custom partitioner
class TablePartitioner extends Partitioner {
  override def numPartitions: Int = 2
  override def getPartition(key: Any): Int = {
    val tableName = key.asInstanceOf[String]
    if(tableName == "EU") 0
    else 1
  }
}
