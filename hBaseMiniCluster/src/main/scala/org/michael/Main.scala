package org.michael

import java.util
import collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

/**
 * Play around with HBase
 */
object Main extends App {

  val tableNameString = "myFirstTable"
  val tableName = TableName.valueOf(tableNameString)
  val rowKey = "rowkey".getBytes
  val colFamily = "colF".getBytes
  val colQualifier = "id1".getBytes
  val colValue = "one".getBytes

  println("Create Configuration")
  val hBaseConf: Configuration = HBaseConfiguration.create()
  hBaseConf.addResource("core-site.xml")
  hBaseConf.addResource("hdfs-site.xml")
  hBaseConf.addResource("hbase-site.xml")

  println("Create Connection")
  val connection: Connection = ConnectionFactory.createConnection(hBaseConf)

  println("Create Admin")
  val admin: Admin = connection.getAdmin()

  println("Check if table exists:")
  val tableNotYetExists: Boolean = admin.tableExists(tableName)
  println(s"Table " + tableName.toString + " exists? " + tableNotYetExists)

  println("Create Table")
  val colFamilyBuild = ColumnFamilyDescriptorBuilder.newBuilder(colFamily).build()
  val tableDescriptorBuild = TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(colFamilyBuild).build()
  admin.createTable(tableDescriptorBuild)

  println("Check if table exists:")
  val tableExists: Boolean = admin.tableExists(tableName)
  println(s"Table " + tableName.toString + " exists? " + tableExists)

  println("List Tables:")
  val listTables2: util.List[TableDescriptor] = admin.listTableDescriptors
  listTables2.foreach(htd => println(htd.getTableName)) // required: collection.JavaConversions._

  println("Get a Table handle")
  val table: Table = connection.getTable(tableName)

  println("Write a row into table")
  val thePut: Put = new Put(rowKey)
  thePut.addColumn(colFamily, colQualifier, colValue)
  table.put(thePut)

  println("Query a row out of table:")
  val theGet: Get = new Get(rowKey)
  val result = table.get(theGet)
  val value: Array[Byte] = result.value()
  println(Bytes.toString(value))

  println("Scan table:")
  val theScan = new Scan()
  val scanResults: ResultScanner = table.getScanner(theScan)
  for(scanResult <- scanResults) {println(Bytes.toString(scanResult.getValue(colFamily, colQualifier)))}
  scanResults.close()

  println("Delete table")
  admin.disableTable(tableName)
  admin.deleteTable(tableName)
  println("Check if table exists:")
  val tableNotExists: Boolean = admin.tableExists(tableName)
  println(s"Table " + tableName.toString + " exists? " + tableNotExists)

  println("Close table and connection")
  table.close()
  connection.close()
}
