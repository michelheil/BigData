package org.michael.spark

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.mutable


class GracefulShutdownTest extends FlatSpec with Matchers with BeforeAndAfter {

  val NotGracefulStopKey = "notGracefulStop"
  val GracefulStopKey = "gracefulStop"

  val conf = new SparkConf().setAppName("SparkGracefulShutdownTest").setMaster("local[1]")
  var streamingContext: StreamingContext = null
  var wasStopped = false
  val dataQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()

  before {
    wasStopped = false
    println("Create streaming context")
    streamingContext = new StreamingContext(conf, Durations.seconds(1))
    println("Set log level to WARN")
    val log: Logger = LogManager.getRootLogger()
    log.setLevel(Level.WARN)
  }

  after {
    if (!wasStopped) {
      streamingContext.stop(stopSparkContext = true, stopGracefully = true)
    }
  }

  "remaining data" should "be treated when context is stopped gracefully" in {
    consumeData(true, GracefulStopKey)
    // sleep - gives some time to accumulate remaining processed data
    Thread.sleep(6000)

    // all remaining tasks will be processed
    NumbersTracker.collectedData(GracefulStopKey) should contain theSameElementsAs(0 to 49)
  }

  "remaining data" should "not be processed when the context is not stopped gracefully" in {
    consumeData(false, NotGracefulStopKey)
    // sleep - gives some time to accumulate remaining processed data
    Thread.sleep(6000)

    // when the context is not stopped gracefully, remaining data and pending tasks
    // won't be executed, so automatically some of the last RDDs to consume
    // aren't  processed
    NumbersTracker.collectedData(NotGracefulStopKey) should contain noneOf(30, 31,
      32, 33, 34, 35, 36, 37, 38, 39, 40,
      41, 42, 43, 44, 45, 46, 47, 48, 49)
  }

  private def consumeData(stopGracefully: Boolean, trackedNumbersKey: String): Unit = {
    val itemsInBatch = 10
    for (index <- 0 until 5) {
      val start = itemsInBatch*index
      val data = start to start+itemsInBatch-1
      println("Create dataQueue" + index)
      dataQueue += streamingContext.sparkContext.parallelize(data)
    }
    streamingContext.queueStream(dataQueue, oneAtATime = true)
      .foreachRDD(rdd => {
        rdd.foreach(number => {
          println(System.nanoTime())
          // after some tests, Spark's accumulator doesn't contain
          // data processed after graceful stop
          println(number)
          NumbersTracker.collectedData(trackedNumbersKey) += number
        })
      })

    new Thread(new Runnable() {
      override def run(): Unit = {
        println("Go to sleep for 3 seconds")
        Thread.sleep(3000)
        val stopSparkContext = true
        println("Stop streamingContext")
        streamingContext.stop(stopSparkContext, stopGracefully)
        wasStopped = true
      }
    }).start()

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}

object NumbersTracker {

  val collectedData: mutable.Map[String, mutable.ListBuffer[Int]] =
    mutable.HashMap.empty[String, mutable.ListBuffer[Int]]

  collectedData += ("notGracefulStop" -> mutable.ListBuffer.empty[Int])
  collectedData += ("gracefulStop"  -> mutable.ListBuffer.empty[Int])

}
