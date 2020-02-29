package org.michael.big.data.spark.hbase

import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog
import org.apache.spark.sql.SparkSession

case class Employee(key: String, firstName: String, lastName: String, middleName: String,
                    addressLine: String, city: String, state: String, zipCode: String)

object Main extends App {

  // as pre-requisites the table 'employee' with column families 'person' and 'address should exist

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

  val data = Seq(
    Employee("1","Abby","Smith","K","3456main","Orlando","FL","45235"),
    Employee("2","Amaya","Williams","L","123Orange","Newark","NJ","27656"),
    Employee("3","Alchemy","Davis","P","Warners","Sanjose","CA","34789")
  )

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("HBaseConnector")
    .getOrCreate()

  import spark.implicits._
  val df = spark.sparkContext.parallelize(data).toDF

  df.write.options(
    Map(HBaseTableCatalog.tableCatalog -> catalog)) // HBaseTableCatalog.newTable -> "3" // If defined and larger than 3, a new table will be created with the nubmer of region specified.
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .save()

}
