package org.michael.big.data.spark.monitoring
import java.util.concurrent.ExecutorService

import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.ForeachWriter

trait HBaseForeachWriter[RECORD] extends ForeachWriter[RECORD] {

  val tableName: String
  val hbaseConfResources: Seq[String]

  def pool: Option[ExecutorService] = None

  def user: Option[User] = None

  @transient private var hTable: Table = _
  @transient private var connection: Connection = _

  private var localPartition: Long = _
  private var localVersion: Long = _


  override def open(partitionId: Long, version: Long): Boolean = {
    localPartition = partitionId
    localVersion = version

    println("Creating HBase connection on executor...")
    connection = createConnection()
    println(s"Get table ${tableName}...")
    hTable = getHTable(connection)

    println(s"... called 'open' for partitionId: ${partitionId} and version: ${version}.")
    true
  }

  override def process(record: RECORD): Unit = {
    val put = toPut(record)
    hTable.put(put)
  }

  override def close(errorOrNull: Throwable): Unit = {
    if(errorOrNull == null) {
      println("Closing local HBase.Table and HBase connection...")
      hTable.close()
      connection.close()
      println(s"Closing partition ${localPartition} and version ${localVersion}.")
    } else {
      println("Query failed with: " + errorOrNull)
    }
  }

  def createConnection(): Connection = {
    val hbaseConfig = HBaseConfiguration.create()
    hbaseConfResources.foreach(hbaseConfig.addResource)
    ConnectionFactory.createConnection(hbaseConfig, pool.orNull, user.orNull)
  }

  def getHTable(connection: Connection): Table = {
    connection.getTable(TableName.valueOf(tableName))
  }

  def toPut(record: RECORD): Put

}
