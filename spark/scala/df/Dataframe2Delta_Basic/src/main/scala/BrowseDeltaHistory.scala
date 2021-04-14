import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

object BrowseDeltaHistory extends App {

  val deltaPath = "file:///tmp/delta/table"

  val spark = SparkSession.builder()
    .appName("BrwoseDeltaHistory")
    .master("local[*]")
    .getOrCreate()

  val deltaTable = DeltaTable.forPath(deltaPath)

  deltaTable
    .history(30)
    .select("version", "timestamp", "operation", "readVersion")
    .show(false)

  val deltaTableAsOfTs = spark.read.format("delta").option("timestampAsOf", "2021-04-14 12:05:40").load(deltaPath)
  deltaTableAsOfTs.show(false)

  val deltaTableAsOfVersion = spark.read.format("delta").option("versionAsOf", "1").load(deltaPath)
  deltaTableAsOfVersion.show(false)

}