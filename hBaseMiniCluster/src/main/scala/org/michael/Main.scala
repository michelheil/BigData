package org.michael

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Get, Put}
import org.apache.hadoop.hbase.util.Bytes

/**
 * Play around with HBase
 */
object Main extends App {

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

  println("List Tables")
  val listTables = admin.listTables()
  listTables.foreach(println)

  val tableName = TableName.valueOf("myFirstTable")
  val table = connection.getTable(tableName)

  val thePut: Put = new Put(Bytes.toBytes("rowkey1"))

  thePut.addColumn(Bytes.toBytes("colF"),Bytes.toBytes("id1"),Bytes.toBytes("one"))
  table.put(thePut)

  val theGet: Get = new Get(Bytes.toBytes("rowkey1"))
  val result = table.get(theGet)
  val value = result.value()
  println(Bytes.toString(value))
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