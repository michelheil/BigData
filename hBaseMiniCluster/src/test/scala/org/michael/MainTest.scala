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
    utility.startMiniCluster(3)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    utility.shutdownMiniCluster()
    super.afterAll()
  }

  feature("Test") {
    scenario(("Test1")) {
    val table = utility.createTable(Bytes.toBytes("myTestTable"), family)

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
  }}
}
