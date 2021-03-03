import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{SparkSession, Dataset, Row, SaveMode}
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQuery}
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object MinHashLSH_Streaming extends App {

  val spark = SparkSession.builder()
    .appName("StructuredStreamingML")
    .master("local[*]")
    .getOrCreate()

  val dfA = spark.createDataFrame(Seq(
    (0, Vectors.sparse(6, Seq((0, 1.0), (1, 1.0), (2, 1.0)))),
    (1, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (4, 1.0)))),
    (2, Vectors.sparse(6, Seq((0, 1.0), (2, 1.0), (4, 1.0))))
  )).toDF("id", "features")
  dfA.show(false)

  val dfB = spark.createDataFrame(Seq(
    //(3, Vectors.sparse(6, Seq((1, 1.0), (3, 1.0), (5, 1.0)))),
    //(4, Vectors.sparse(6, Seq((2, 1.0), (3, 1.0), (5, 1.0)))),
    (5, Vectors.sparse(6, Seq((1, 1.0), (2, 1.0), (4, 1.0))))
  )).toDF("id", "features")
  dfB.show(false)


  val mh = new MinHashLSH()
    .setNumHashTables(5)
    .setInputCol("features")
    .setOutputCol("hashes")

  val model = mh.fit(dfA)
  model.write.overwrite().save("/tmp/structuredML")


  val model2 = PipelineModel.read.load("/tmp/structuredML")

  // Feature Transformation
  println("The hashed dataset where hashed values are stored in the column 'hashes':")
  model.transform(dfA).show()
  model2.transform(dfA).show()
  model.transform(dfB).show()

  // Compute the locality sensitive hashes for the input rows, then perform approximate
  // similarity join.
  // We could avoid computing hashes by passing in the already-transformed dataset, e.g.
  // `model.approxSimilarityJoin(transformedA, transformedB, 0.6)`
  println("Approximately joining dfA and dfB on Jaccard distance smaller than 0.6:")
  model.approxSimilarityJoin(dfA, dfB, 0.6, "JaccardDistance")
    .select(col("datasetA.id").alias("idA"),
      col("datasetB.id").alias("idB"),
      col("JaccardDistance")).show()

  /*
  //spark.sparkContext.setLogLevel("DEBUG")
  val df = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .option("maxOffsetsPerTrigger", "10")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("CAST(value AS STRING) as value", "timestamp")

  val query: StreamingQuery = df.writeStream
    //.format("console")
    .outputMode("append")
    .foreachBatch((ds: Dataset[Row], batchId: Long) => {
      println(ds.count())
      ds.write.format("csv").mode(SaveMode.Append).save("/tmp/test/tmp.csv")
    })
    //.option("truncate", "false")
    //.option("checkpointLocation", "/home/michael/sparkCheckpoint")
    .queryName("StackoverflowTest")
    //.trigger(Trigger.ProcessingTime(1000))
    .start()

  query.awaitTermination()
*/
}


