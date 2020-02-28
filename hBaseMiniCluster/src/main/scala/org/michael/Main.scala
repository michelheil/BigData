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

  // install hadoop and HBase as written in
  // https://computingforgeeks.com/how-to-install-apache-hadoop-hbase-on-ubuntu/

  // execute "bash start.sh" in ~/GitHubRepositories/ToolCommands/hadoop
  // for testing with hBaseMiniCluster ensure that Hbase, local-master-backup and local-regionservers are not running

  // create table through "hbase shell"
  // create 'myFirstTable', 'colF'

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

  println("Close table")
  table.close()
}





/*
  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(TableName.valueOf( Bytes.toBytes("emostafa:test_table") ) )

  // Put example
  var put = new Put(Bytes.toBytes("row1"))
  put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("test_column_name"), Bytes.toBytes("test_value"))
  put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("test_column_name2"), Bytes.toBytes("test_value2"))
  table.put(put)

  // Get example
  println("Get Example:")
  var get = new Get(Bytes.toBytes("row1"))
  var result = table.get(get)
  printRow(result)

  //Scan example
  println("\nScan Example:")
  var scan = table.getScanner(new Scan())
  scan.asScala.foreach(result => {
      printRow(result)
  })

  table.close()
  connection.close()
*/


/*
//CHECK ishassan/build.sbt as well
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.conf.Configuration
import scala.collection.JavaConverters._

object ScalaHBaseExample extends App{

  def printRow(result : Result) = {
    val cells = result.rawCells();
    print( Bytes.toString(result.getRow) + " : " )
    for(cell <- cells){
      val col_name = Bytes.toString(CellUtil.cloneQualifier(cell))
      val col_value = Bytes.toString(CellUtil.cloneValue(cell))
      print("(%s,%s) ".format(col_name, col_value))
    }
    println()
  }

  val conf : Configuration = HBaseConfiguration.create()
  /*
  From http://hbase.apache.org/0.94/book/zookeeper.html
  A distributed Apache HBase (TM) installation depends on a running ZooKeeper cluster. All participating nodes and clients
  need to be able to access the running ZooKeeper ensemble. Apache HBase by default manages a ZooKeeper "cluster" for you.
  It will start and stop the ZooKeeper ensemble as part of the HBase start/stop process. You can also manage the ZooKeeper
  ensemble independent of HBase and just point HBase at the cluster it should use. To toggle HBase management of ZooKeeper,
  use the HBASE_MANAGES_ZK variable in conf/hbase-env.sh. This variable, which defaults to true, tells HBase whether to
  start/stop the ZooKeeper ensemble servers as part of HBase start/stop.
  */
  val ZOOKEEPER_QUORUM = "WRITE THE ZOOKEEPER CLUSTER THAT HBASE SHOULD USE"
  conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);

  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(TableName.valueOf( Bytes.toBytes("emostafa:test_table") ) )

  // Put example
  var put = new Put(Bytes.toBytes("row1"))
  put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("test_column_name"), Bytes.toBytes("test_value"))
  put.addColumn(Bytes.toBytes("d"), Bytes.toBytes("test_column_name2"), Bytes.toBytes("test_value2"))
  table.put(put)

  // Get example
  println("Get Example:")
  var get = new Get(Bytes.toBytes("row1"))
  var result = table.get(get)
  printRow(result)

  //Scan example
  println("\nScan Example:")
  var scan = table.getScanner(new Scan())
  scan.asScala.foreach(result => {
      printRow(result)
  })

  table.close()
  connection.close()
}

 */