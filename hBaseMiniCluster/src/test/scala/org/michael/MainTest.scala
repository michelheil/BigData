package org.michael

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.client.{Get, Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.{BeforeAndAfterAll, FeatureSpec}

class MainTest extends FeatureSpec with BeforeAndAfterAll {
  var utility: HBaseTestingUtility = _
  val family: Array[Byte] = "CF".getBytes
  val qualifier: Array[Byte] = "CQ".getBytes
  val value: Array[Byte] = "data".getBytes

  override def beforeAll(): Unit = {
    utility = new HBaseTestingUtility
    utility.startMiniCluster(1,3, true)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    utility.shutdownMiniCluster()
    super.afterAll()
  }

  feature("General HBase test using mini cluster") {
    scenario(("creation of table, putting and getting row")) {
      info("Ensure that only dfs and yarn is running.")
      info("Hbase, local-master-backup and local-regionservers should not run.")
      info("Scripts can be found at ~/GitHubRepositories/ToolCommands/hadoop")

      val table = utility.createTable(Bytes.toBytes("myTestTable"), family)
      // HBase version 2.2.3: createTable(TableName.valueOf("myTestTable"), family)
      val putKey = new Put(Bytes.toBytes("ROWKEY-1"))
      putKey.addColumn(family, qualifier, value)
      table.put(putKey)
      //val obj = new HBaseTestObject()
      //obj.setRowKey("ROWKEY-1")
      //obj.setData1("DATA-1")
      // MyHBaseDAO.insertRecord(table, obj)
      val getKey = new Get(Bytes.toBytes("ROWKEY-1"))
      getKey.addColumn(family, qualifier)
      val result: Result = table.get(getKey)
      assertResult(Bytes.toString(result.getRow))("ROWKEY-1")
      assertResult(Bytes.toString(result.value))("DATA-1")
    }
  }
}
