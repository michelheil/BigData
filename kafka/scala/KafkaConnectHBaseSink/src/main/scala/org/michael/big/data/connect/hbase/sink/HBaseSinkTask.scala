package org.michael.big.data.connect.hbase.sink

import java.util
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}
import org.slf4j.{Logger, LoggerFactory}

class HBaseSinkTask extends SinkTask {

  val VERSION = "0.0.1"
  private val log: Logger = LoggerFactory.getLogger(getClass)

  // HBase meta
  var config: HBaseSinkConfig = _
  var connection: Connection = _

  // Table meta
  var tableHandle: Table = _
  var cF: Array[Byte] = _
  var cQ: Array[Byte] = _

  /**
   * Start the Task. This should handle any configuration parsing and one-time setup of the task.
   *
   * @param props initial configuration
   */
  override def start(props: util.Map[String, String]): Unit = {

    log.info("---------- BRIEFTAUBE ----------")
    log.info(s"Show parsed configuration in ${getClass.getName}.start: ${props}")

    // get configuration
    this.config = new HBaseSinkConfig(HBaseSinkConfig.config(), props)
    log.info("---------- BRIEFTAUBE ----------")
    log.info(s"Parsed HBase table name: ${config.getString(HBaseSinkConfig.TABLE_NAME_CONFIG)}")

    // get configuration on column family and qualifier
    cF = config.getString(HBaseSinkConfig.COLUMN_FAMILY_CONFIG).getBytes()
    cQ = config.getString(HBaseSinkConfig.COLUMN_QUALIFIER_CONFIG).getBytes()

    // Create connection to HBase
    val hBaseConf: Configuration = HBaseConfiguration.create()
    hBaseConf.addResource("hbase-site.xml") // eventuell hier die einzelnen Werte von hbase-site und anderen -site XMLs als config einfuegen
    hBaseConf.addResource("core-site.xml") // eventuell hier die einzelnen Werte von hbase-site und anderen -site XMLs als config einfuegen
    this.connection = ConnectionFactory.createConnection(hBaseConf)

    // Create HBase Admin
    val admin: Admin = this.connection.getAdmin()

    // Check if table exists
    val tableName: TableName = TableName.valueOf(config.getString(HBaseSinkConfig.TABLE_NAME_CONFIG))
    val tableExists: Boolean = admin.tableExists(tableName)

    // Create Table if not existing
    if(!tableExists) {
      val colFamilyBuild = ColumnFamilyDescriptorBuilder.newBuilder(cF).build()
      val tableDescriptorBuild = TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(colFamilyBuild).build()
      admin.createTable(tableDescriptorBuild)
    }

    this.tableHandle = connection.getTable(tableName)
  }

  /**
   * Put the records in the sink. Usually this should send the records to the sink asynchronously
   * and immediately return.
   *
   * If this operation fails, the SinkTask may throw a {@link org.apache.kafka.connect.errors.RetriableException} to
   * indicate that the framework should attempt to retry the same call again. Other exceptions will cause the task to
   * be stopped immediately. {@link SinkTaskContext#timeout(long)} can be used to set the maximum time before the
   * batch will be retried.
   *
   * @param records the set of records to send
   */
  override def put(records: util.Collection[SinkRecord]): Unit = {
    records.forEach(record => {
      val rowKey: Array[Byte] = record.timestamp().toString.getBytes()
      val thePut: Put = new Put(rowKey)
      thePut.addColumn(cF, cQ, record.value.toString.getBytes())
      thePut.addColumn(cF, "key".getBytes(), record.key.toString.getBytes())
      thePut.addColumn(cF, "offset".getBytes(), record.kafkaOffset.toString.getBytes())
      tableHandle.put(thePut)
    })
  }


  /**
   * Perform any cleanup to stop this task. In SinkTasks, this method is invoked only once outstanding calls to other
   * methods have completed (e.g., {@link #put(Collection)} has returned) and a final {@link #flush(Map)} and offset
   * commit has completed. Implementations of this method should only need to perform final cleanup operations, such
   * as closing network connections to the sink system.
   */
  override def stop(): Unit = {
    tableHandle.close()
    connection.close()
  }

  override def version(): String = VERSION
}

