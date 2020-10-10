package org.michael.big.data.spark.monitoring

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

object MainBootstrap extends App
  with ConfLoader {

  // ensure that the folder where the conf files are located are marked as project resources
  val conf = loadConfigFromPath(getClass.getResource("/").getPath)

  // create SparkStreaming Job that writes from Kafka to Kafka (use Structured API)
  val spark = SparkSession.builder()
    .master(conf.getString("spark.app.master"))
    .appName(conf.getString("spark.app.name"))
    .getOrCreate()

  // read from Kafka
  val ds1 = spark.readStream
    .format("kafka")
    .option(Const.KafkaConf.KAFKA_BOOTSTRAP_SERVERS, conf.getString(Const.KafkaConf.KAFKA_BOOTSTRAP_SERVERS))
    .option("subscribe", conf.getString("kafka.input.topic"))
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()

  // add monitoring
  val monitorListener = new MonitorListener
  spark.streams.addListener(monitorListener)

  // write to HBase
  // create 'hbase-table-name', 'myCF'
  val foreachWriterRow: HBaseForeachWriter[Row] = new HBaseForeachWriter[Row] {
    override val tableName: String = "hbase-table-name"
    // cluster files, assuming it is in resources
    override val hbaseConfResources: Seq[String] = Seq("core-site.xml", "hbase-site.xml", "hdfs-site.xml")

    override def toPut(record: Row): Put = {
      println(s"Trying to write ${record} to HBase...")
      val key = "myKey" // record.getString(0)
      val columnFamilyName : String = "myCF"
      val columnName : String = "myColumn"
      val columnValue = "myValue" // record.getString(1)

      val p: Put = new Put(Bytes.toBytes(key))
      // Add columns
      p.addColumn(
        Bytes.toBytes(columnFamilyName), // colFamily
        Bytes.toBytes(columnName), // colQualifier
        Bytes.toBytes(columnValue) // volValue
      )
      p
    }
  }

  // write to Kafka
  ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .writeStream
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .format("kafka")
    .option("checkpointLocation", conf.getString("spark.app.checkpoint.location.dir"))
    .option(Const.KafkaConf.KAFKA_BOOTSTRAP_SERVERS, conf.getString(Const.KafkaConf.KAFKA_BOOTSTRAP_SERVERS))
    .option("topic", conf.getString("kafka.output.topic"))
    .start()

  val query: StreamingQuery = ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream
    .foreach(foreachWriterRow).start()

  spark.streams.awaitAnyTermination()
}
