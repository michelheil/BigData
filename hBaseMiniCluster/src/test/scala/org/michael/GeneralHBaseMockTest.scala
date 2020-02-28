package org.michael

import org.apache.hadoop.hbase.{HBaseTestingUtility, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.{BeforeAndAfterAll, FeatureSpec}

class GeneralHBaseMockTest extends FeatureSpec with BeforeAndAfterAll {
  val valueRaw = "data"
  val rowName = "ROWKEY-1"

  var utility: HBaseTestingUtility = _
  val family: Array[Byte] = "CF".getBytes
  val qualifier: Array[Byte] = "CQ".getBytes
  val value: Array[Byte] = valueRaw.getBytes

  override def beforeAll(): Unit = {
    utility = new HBaseTestingUtility()
    utility.startMiniCluster(1, 3, true)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    utility.shutdownMiniCluster()
    super.afterAll()
  }


  feature("General HBase test using mini cluster") {
    scenario(("create table, putting row, getting row, check existence, delete table")) {
      info("Ensure that only dfs and yarn is running.")
      info("Hbase, local-master-backup and local-regionservers should not run.")
      info("Scripts can be found at ~/GitHubRepositories/ToolCommands/hadoop")

      // Creating Table
      val table = utility.createTable(TableName.valueOf("myTestTable"), family)

      // Putting Row
      val putKey = new Put(Bytes.toBytes(rowName))
      putKey.addColumn(family, qualifier, value)
      table.put(putKey)

      // Getting Row
      val getKey = new Get(Bytes.toBytes(rowName))
      getKey.addColumn(family, qualifier)
      val result: Result = table.get(getKey)

      assertResult(rowName)(Bytes.toString(result.getRow))
      assertResult(valueRaw)(Bytes.toString(result.value))

      // Check if table exists
      val admin = utility.getAdmin()
      val tablesExists = admin.tableExists(TableName.valueOf("myTestTable"))

      // Assertions
      assertResult(true)(tablesExists)

      // delete table
      admin.disableTable(TableName.valueOf("myTestTable"))
      admin.deleteTable(TableName.valueOf("myTestTable"))
      val tablesNotExists = admin.tableExists(TableName.valueOf("myTestTable"))
      assertResult(false)(tablesNotExists)
    }
  }
}
