import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AnalyzeDataSkew extends App {

  // create Spark Session
  val spark = SparkSession
    .builder
    .appName("AnalyzeDataSkew")
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._

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

  // Define case class (Encoder) for the Dataset creation
  case class SkewExample(constDist: Int, evenDist: Int)

  val r = new scala.util.Random(1337)

  val data = for (i <- 1 to 1000000)
    yield (SkewExample(1, r.nextInt(100)))

  val df = spark.createDataset(data)

  // Add skewed column to the Dataframe
  val skewDf = df.withColumn("skewDist",
    when(col("evenDist") < 80, lit(0))
      .when(col("evenDist") < 85, lit(1))
      .when(col("evenDist") < 90, lit(2))
      .when(col("evenDist") < 95, lit(3))
      .otherwise(lit(4))
  )

  skewDf.createOrReplaceTempView("skewTab")

  spark.sparkContext.setJobDescription("Experiment 1A: Order on skewed column")
  spark.conf.set("spark.sql.shuffle.partitions", 8)
  skewDf.orderBy(col("skewDist")).foreach(_ => ())

  spark.sparkContext.setJobDescription("Experiment 1B: Order on evenly distributed column")
  spark.conf.set("spark.sql.shuffle.partitions", 8)
  skewDf.orderBy(col("evenDist")).foreach(_ => ())

  spark.sparkContext.setJobDescription("Experiment 2A: Aggregate on skewed column while forcing partitions")
  spark.conf.set("spark.sql.shuffle.partitions", 8)
  skewDf.repartition(5, col("skewDist")) // simulating data which is partitioned when reading from a source (HDFS file, Kafka partitions)
    .groupBy(col("skewDist"))
    .agg(max(col("evenDist")).as("maxEven"))
    .foreach(_ => ())

  spark.sparkContext.setJobDescription("Experiment 2B: Aggregate on evenly distributed column while forcing partitions")
  spark.conf.set("spark.sql.shuffle.partitions", 8)
  skewDf.repartition(5, col("evenDist")) // simulating data which is partitioned when reading from a source (HDFS file, Kafka partitions)
    .groupBy(col("evenDist"))
    .agg(max(col("skewDist")).as("maxSkew"))
    .foreach(_ => ())

  val rs = new scala.util.Random(47)

  // Define case class (Encoder) for the Dataset creation
  case class SmallDataExample(konstVert: Int, gleichVert: Int)

  val smallData = for (i <- 1 to 100)
    yield SmallDataExample(1337, rs.nextInt(5))

  val smallDf = spark.createDataset(smallData)


  // "Disabling" automatic broadcast when joining with a fairly small Dataframe
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1b")
  spark.conf.set("spark.sql.shuffle.partitions", 5)

  spark.conf.set("spark.sql.adaptive.enabled", false)
  spark.conf.set("spark.sql.adaptive.skewJoin.enabled", false)
  spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", false)
  spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", false)

  spark.sparkContext.setJobDescription("Experiment 3A: Join on skewed column with AQE disabled")
  skewDf.repartition(col("skewDist"))
    .join(smallDf, col("skewDist") === col("gleichVert"))
    .write.format("noop").mode("overwrite").save()

  // "Disabling" automatic broadcast when joining with a fairly small Dataframe
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1b")
  spark.conf.set("spark.sql.shuffle.partitions", 5)

  spark.conf.set("spark.sql.adaptive.enabled", false)
  spark.conf.set("spark.sql.adaptive.skewJoin.enabled", false)
  spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", false)
  spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", false)

  spark.sparkContext.setJobDescription("Experiment 3B: Join on evenly distributed column with AQE disabled")
  skewDf.repartition(col("evenDist"))
    .join(smallDf, col("evenDist") === col("gleichVert"))
    .write.format("noop").mode("overwrite").save()

  // mitigates experiment 3
  spark.sparkContext.setJobDescription("Experiment 4: Join on skewed column with replicated smallDF")

  // The higher the number the less data per partition. However, the higher the number the more time it takes to crossJoin.
  // Refer to explode small array which can cause Data spill.
  val levelOfSalt = 5

  // Apply salt to existing key
  val saltedSkewedDf = skewDf
    .withColumn("salt", (lit(levelOfSalt) * rand()).cast("int"))
    .withColumn("salted_skewed", concat(col("skewDist"), lit("-"), col("salt")))
    .drop("salt")

  // Replicate each row in the small Dataframe with salt
  val saltDf = spark.range(levelOfSalt).toDF("salt")
  val replicatedSmallDf = smallDf.crossJoin(saltDf)
    .withColumn("salted_gleich", concat(col("gleichVert"), lit("-"), col("salt")))
    .drop("salt")

  saltedSkewedDf.repartition(col("salted_skewed"))
    .join(replicatedSmallDf, col("salted_skewed") === col("salted_gleich"))
    .write.format("noop").mode("overwrite").save()

  // mitigates experiment 2
  spark.sparkContext.setJobDescription("Experiment 5: Aggregation on skewed salted column")
  spark.conf.set("spark.sql.shuffle.partitions", 8)

  saltedSkewedDf
    // Our assumption was that the skewDf is pre-partitioned by column skewDist.
    // Therefore, we need to have it repartitioned by the salted column.
    .repartition(5, col("salted_skewed"))
    .groupBy(col("salted_skewed"), col("skewDist"))
    .agg(max(col("evenDist")).as("maxEven"))
    .groupBy(col("skewDist"))
    .agg(max(col("maxEven")).as("maxEven"))
    .foreach(_ => ())


  println("Work is done!")
  Thread.sleep(600000)
}
