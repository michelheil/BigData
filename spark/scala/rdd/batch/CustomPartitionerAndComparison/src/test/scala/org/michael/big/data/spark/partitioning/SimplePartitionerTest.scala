package org.michael.big.data.spark.partitioning

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, Partitioner, TaskContext}
import org.scalatest.{FunSpec, MustMatchers}

class SimplePartitionerTest extends FunSpec with MustMatchers {

  val testSpark = SparkSession.builder()
    .appName("Partitioning")
    .master("local[*]")
    .getOrCreate()

  val inputData = Seq(
    ("EU", "testData1"),
    ("EU", "testData2"),
    ("EU", "testData3"),
    ("US", "testData5"),
    ("US", "testData6")
  )

  // create RDD
  val rdd: RDD[(String, String)] = testSpark.sparkContext.parallelize(inputData)

  // ensure that one Partition is empty
  val rddWithEmptyPartitions = rdd.partitionBy(new myPartitioner)
  println(s"Number of partitions of rddWithEmptyPartitions: ${rddWithEmptyPartitions.getNumPartitions} using Partitioner: ${rddWithEmptyPartitions.partitioner}.")

  // show distribution of data across partitions
  rddWithEmptyPartitions.foreachPartition(iter => {
    iter.foreach(k => println(s"Partition: ${TaskContext.getPartitionId()}, Key: ${k._1}, Value: ${k._2}"))
  })

  // count messages per partition
  rddWithEmptyPartitions.mapPartitionsWithIndex((partIndex, data) => {
    println(s"Partition: $partIndex contains ${data.size} message.")
    data
  })

  // bring metaData into RDD
  val bigRDD = rddWithEmptyPartitions.mapPartitions(part => {
    val x = part.size
    part.map(row => {
      val y: (String, String) = row
      (x,row)
    })
  })
  println(s"Number of partitions of bigRDD: ${bigRDD.getNumPartitions} using Partitioner: ${bigRDD.partitioner}.")
  bigRDD.foreachPartition(iter => {
    iter.foreach(k => println(s"Partition: ${TaskContext.getPartitionId()}, Key: ${k._1}, Value: ${k._2}"))
  })

  // filter out empty partition
  //
  //val rddWoEmptyPartitions = rddWithEmptyPartitions.mapPartitionsWithIndex((partIndex, data) => {
  //  data
  //})


  // show messages are still at the same partition and same partitioner still aplies
//  println(s"Partitioner: ${rddWoEmptyPartitions.partitioner}.")
//  rddWoEmptyPartitions.foreachPartition(iter => {
//    iter.foreach(k => println(s"Partition: ${TaskContext.getPartitionId()}, Key: ${k._1}, Value: ${k._2}"))
//  })


}

// define custom partitioner
class myPartitioner extends Partitioner {
  override def numPartitions: Int = 3
  override def getPartition(key: Any): Int = {
    val region = key.asInstanceOf[String]
    if(region == "EU") 0
    else if (region == "US") 1
    else 2
  }
}
