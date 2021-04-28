import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel

import sys.process._

object AnalyzeDataSkew extends App {

  // refer to original raw csv file
  val covidCsvInputFile = "/home/michael/Downloads/COVID-19_Case_Surveillance_Public_Use_Data_with_Geography.csv"

  // clear delta output path to make running this application idempotent
  val deltaPath = "/tmp/delta/parts"
  ("rm -rf " + deltaPath)!

  // create Spark Session
  val spark = SparkSession
    .builder
    .appName("AnalyzeDataSkew")
    .master("local[*]")
    .getOrCreate()

  // set configurations
  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.set("spark.sql.adaptive.enabled", false)
  spark.conf.set("spark.sql.adaptive.skewJoin.enabled", false)
  spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "5m")

//  spark.conf.set("spark.sql.shuffle.partitions", 18)

  // check configurations
  println("autoBroadcastJoinThreshold: " + spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
  println("maxPartitionBytes: " + spark.conf.get("spark.sql.files.maxPartitionBytes"))
  println("openCostInBytes: " + (spark.conf.get("spark.sql.files.openCostInBytes").toLong / 1048576) + "MB")
  println("ExecutorMemoryStatus" + spark.sparkContext.getExecutorMemoryStatus)

  // create dimTable
  import spark.implicits._
  val dimTable = Seq(
    ("NA", "foo0"),
    ("18 to 49 years", "foo1"),
    ("0 - 17 years", "foo2"),
    ("50 to 64 years", "foo3"),
    ("65+ years", "foo4"),
    ("Missing", "foo5")
  ).toDF("dim_age_group", "dimValue")
  dimTable.show(false)

  spark.sparkContext.setJobDescription("Step 1 - Read original csv file")
  val rawDf = spark.read.option("header", true).csv(covidCsvInputFile)

  spark.sparkContext.setJobDescription("Step 2 - Write original data as Delta Table")
  rawDf.write.format("delta").mode("overwrite").partitionBy("age_group").save(deltaPath)

  spark.sparkContext.setJobDescription("Step 3 - Load Delta Table")
  val rawDeltaDf = spark.read.format("delta").load(deltaPath)
  // spark.sparkContext.setJobDescription("Step 3 - Persist original data")
  // rawDf.persist(StorageLevel.MEMORY_AND_DISK).count() // action to eagerly persist

  spark.sparkContext.setJobDescription("Step 4 - Group by age_group")
  rawDeltaDf.groupBy(col("age_group")).count().show(false)

//  spark.sparkContext.setJobDescription("Step 5 - Cache partitionedBy age_group")
//  val partitionedDf = rawDf.repartition(col("age_group"))
//  partitionedDf.cache().count() // action to eagerly persist

  spark.sparkContext.setJobDescription("Step 6 - Join and Foreach")
  rawDeltaDf.join(dimTable, col("age_group") === col("dim_age_group")).foreach(_ => ())


  println("Work is done!")
  Thread.sleep(600000)
}
