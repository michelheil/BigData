import org.apache.spark.sql.SparkSession

object AnalyzeDataSpill extends App {

  // refer to original raw csv file
  // Source: https://data.cdc.gov/Case-Surveillance/COVID-19-Case-Surveillance-Public-Use-Data-with-Ge/ynhu-f2s2
  val covidCsvInputFile = "/home/michael/Downloads/COVID-19_Case_Surveillance_Public_Use_Data_with_Geography.csv"

  // create Spark Session
  val spark = SparkSession
    .builder
    .appName("AnalyzeDataSpill")
    .master("local[4]")
    .getOrCreate()

  // set configurations
  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.set("spark.sql.shuffle.partitions", 4)
  // spark.conf.set("spark.sql.autoBroadcastJoinThreshold")
  spark.conf.set("spark.memory.fraction", "0.6")

  // Turn Off main Spark 3 Features dealing with Spill/Skew/Shuffle
  spark.conf.set("spark.sql.adaptive.enabled", false)
  spark.conf.set("spark.sql.adaptive.skewJoin.enabled", false)
  spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", false)
  spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", false)
//  spark.conf.set("spark.sql.files.maxPartitionBytes", "16mb")

  println(java.lang.Runtime.getRuntime.maxMemory())


// Experiment 1
  // 1GB executor memory
  // 0.6 fraction

  // Check value on maxPartitionBytes to understand how many task are being used to read the csv-file
  println("spark.sql.files.maxPartitionBytes: " + spark.conf.get("spark.sql.files.maxPartitionBytes"))

  spark.sparkContext.setJobDescription("Experiment 1: Read Data")
  val rawDf = spark.read.option("header", true).csv(covidCsvInputFile)

  spark.sparkContext.setJobDescription("Experiment 1: Cache Data")
  rawDf.cache().count() // eagerly cache Dataframe

// Experiment 2
  import org.apache.spark.sql.functions._
  // 1GB executor memory
  // 0.6 fraction

  // Set the number of shuffle partitions to the number of cores.
  spark.conf.set("spark.sql.shuffle.partitions", 4)
  println("spark.sql.shuffle.partitions: " + spark.conf.get("spark.sql.shuffle.partitions"))

  spark.sparkContext.setJobDescription("Experiment 2: Read Data")
  val dfExp2 = spark.read.option("header", true).csv(covidCsvInputFile)

  // Repartition (RoundRobin) of the data does not cause any data spill.
  spark.sparkContext.setJobDescription("Experiment 2: Repartition(4) and apply Foreach on Data")
  dfExp2.repartition(4).foreach(_ => ())

  // Adding a Column with an Array of just two Elements to the data and exploding that column
  // Here only transformations are applied and not actions
  val prettySmallArray = (1 to 2).toArray
  val explodedDf = dfExp2
    .withColumn("columnToBeExploded", lit(prettySmallArray))
    .select(col("*"), explode(col("columnToBeExploded")))

  // Repartition the exploded data into four partitions on a round-robin basis
  spark.sparkContext.setJobDescription("Experiment 2: Explode Array(2), Repartition(4) and apply Foreach on Data")
  explodedDf.repartition(4).foreach(_ => ())

  // Apply a wide transformation `distinct` to the exploded data which causes a full shuffle of the data
  spark.sparkContext.setJobDescription("Experiment 2: Explode Array(2), Distinct and apply Foreach on Data")
  explodedDf.distinct().foreach(_ => ())






// Experiment 3
  import org.apache.spark.sql.functions._
  // 1GB executor memory
  // 0.6 fraction

  spark.conf.set("spark.sql.shuffle.partitions", 6)
  println("spark.sql.shuffle.partitions: " + spark.conf.get("spark.sql.shuffle.partitions"))

  // Create large Dataframe
  spark.sparkContext.setJobDescription("Experiment 3: Read Data - Full")
  val dfExp3 = spark.read.option("header", true).csv(covidCsvInputFile)

  // Create a small Dataframe with only 10 rows.
  spark.sparkContext.setJobDescription("Experiment 3: Read Data - limit 10")
  val dfSmall = dfExp3.limit(10)

  // Use join hint to avoid the small Dataframe being auto broadcasted.
  spark.sparkContext.setJobDescription("Experiment 3: Sort Merge Join on skewed Data")
  dfExp3.hint("SHUFFLE_MERGE").join(dfSmall, "age_group").foreach(_ => ())




// Experiment 4
  import org.apache.spark.sql.functions._
  // 1GB executor memory
  // 0.6 fraction

  spark.conf.set("spark.sql.shuffle.partitions", 6)
  println("spark.sql.shuffle.partitions: " + spark.conf.get("spark.sql.shuffle.partitions"))

  spark.sparkContext.setJobDescription("Experiment 4: Read Data")
  val dfExp4 = spark.read.option("header", true).csv(covidCsvInputFile)

  // Apply a wide transformation `orderBy` which causes a full shuffle of the data
  spark.sparkContext.setJobDescription("Experiment 4: OrderBy (wide transformation) on a skewed feature: age_group")
  dfExp4.orderBy("age_group").foreach(_ => ())

  println("Work is done!")
  Thread.sleep(600000)
}
