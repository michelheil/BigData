package org.michael.big.data.spark.hbase

import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, ConnectionFactory, TableDescriptorBuilder}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

object Main extends App {

  // as pre-requisites the table 'employee' with column families 'person' and 'address' should exist
  val tableNameString = "default:employee"
  val colFamilyPString = "person"
  val colFamilyAString = "address"
  val tableName = TableName.valueOf(tableNameString)
  val colFamilyP = colFamilyPString.getBytes
  val colFamilyA = colFamilyAString.getBytes

  val hBaseConf = HBaseConfiguration.create()
  val connection = ConnectionFactory.createConnection(hBaseConf);
  val admin = connection.getAdmin();

  println("Check if table 'employee' exists:")
  val tableExistsCheck: Boolean = admin.tableExists(tableName)
  println(s"Table " + tableName.toString + " exists? " + tableExistsCheck)

  if(tableExistsCheck == false) {
    println("Create Table employee with column families 'person' and 'address'")
    val colFamilyBuild1 = ColumnFamilyDescriptorBuilder.newBuilder(colFamilyP).build()
    val colFamilyBuild2 = ColumnFamilyDescriptorBuilder.newBuilder(colFamilyA).build()
    val tableDescriptorBuild = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(colFamilyBuild1)
      .setColumnFamily(colFamilyBuild2)
      .build()
    admin.createTable(tableDescriptorBuild)
  }

  // define schema for the dataframe that should be loaded into HBase
  def catalog =
    s"""{
       |"table":{"namespace":"default","name":"employee"},
       |"rowkey":"key",
       |"columns":{
       |"key":{"cf":"rowkey","col":"key","type":"string"},
       |"fName":{"cf":"person","col":"firstName","type":"string"},
       |"lName":{"cf":"person","col":"lastName","type":"string"},
       |"mName":{"cf":"person","col":"middleName","type":"string"},
       |"addressLine":{"cf":"address","col":"addressLine","type":"string"},
       |"city":{"cf":"address","col":"city","type":"string"},
       |"state":{"cf":"address","col":"state","type":"string"},
       |"zipCode":{"cf":"address","col":"zipCode","type":"string"}
       |}
       |}""".stripMargin

  // define some test data
  val data = Seq(
    Employee("1","Abby","Smith","K","3456main","Orlando","FL","45235"),
    Employee("2","Amaya","Williams","L","123Orange","Newark","NJ","27656"),
    Employee("3","Alchemy","Davis","P","Warners","Sanjose","CA","34789")
  )

  // create SparkSession
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("HBaseConnector")
    .getOrCreate()

  // serialize data
  import spark.implicits._
  val df = spark.sparkContext.parallelize(data).toDF

  // write dataframe into HBase
  df.write.options(
    Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "3")) // create 3 regions
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .save()

}
