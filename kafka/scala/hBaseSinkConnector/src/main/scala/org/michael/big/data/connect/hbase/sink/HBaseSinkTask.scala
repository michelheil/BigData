package org.michael.big.data.connect.hbase.sink

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, ColumnFamilyDescriptorBuilder, Connection, ConnectionFactory, Put, Table, TableDescriptorBuilder}
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}
import org.slf4j.{Logger, LoggerFactory}

class HBaseSinkTask extends SinkTask {

  private val log: Logger = LoggerFactory.getLogger(getClass)

  var connection: Connection = _
  var tableHandle: Table = _

  /**
   * Start the Task. This should handle any configuration parsing and one-time setup of the task.
   *
   * @param props initial configuration
   */
  override def start(props: util.Map[String, String]): Unit = {
    val hBaseConf: Configuration = HBaseConfiguration.create()
    hBaseConf.addResource("hbase-site.xml")
    this.connection = ConnectionFactory.createConnection(hBaseConf)

    // Create Admin
    val admin: Admin = this.connection.getAdmin()

    // Check if table exists
    val tableExists: Boolean = admin.tableExists(HBaseSinkTask.tableName)

    if(!tableExists) {
      // Create Table
      val colFamilyBuild = ColumnFamilyDescriptorBuilder.newBuilder(HBaseSinkTask.colFamily).build()
      val tableDescriptorBuild = TableDescriptorBuilder.newBuilder(HBaseSinkTask.tableName).setColumnFamily(colFamilyBuild).build()
      admin.createTable(tableDescriptorBuild)
    }

    this.tableHandle = connection.getTable(HBaseSinkTask.tableName)
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
      val thePut: Put = new Put(record.key().toString.getBytes())
      thePut.addColumn(HBaseSinkTask.colFamily, HBaseSinkTask.colQualifier, record.value().toString.getBytes())
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

  override def version(): String = HBaseSinkTask.VERSION
}

object HBaseSinkTask {

  val VERSION = "0.0.1"

  val tableNameString = "myFirstTable"
  val tableName = TableName.valueOf(tableNameString)
  val rowKey = "rowkey".getBytes
  val colFamily = "colF".getBytes
  val colQualifier = "id1".getBytes
  val colValue = "one".getBytes

}
