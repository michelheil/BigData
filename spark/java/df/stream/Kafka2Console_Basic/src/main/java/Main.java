import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class Main {

  public static void main(String[] args) throws TimeoutException {

    SparkSession spark = SparkSession
        .builder()
        .appName("Testing")
        .config("spark.master", "local")
        .getOrCreate();

    StructType recordSchema = new StructType()
        .add("description", "string")
        .add("location", "string")
        .add("id", "string");
    /*
        .add("title", "string")
        .add("company", "string")
        .add("place", "string")
        .add("date", "string")
        .add("senorityLevel", "string")
        .add("function", "string")
        .add("employmentType", "string")
        .add("industries", "string");
*/
    Dataset<Row> df = spark
        .readStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "test")
        .option("startingOffsets", "earliest")
        .option("kafka.group.id", "test")
        .load();

    df.printSchema();

    StreamingQuery query = df
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .select(from_json(col("value").cast("string"), recordSchema).as("data"))
        .select("data.*")
        .writeStream()
        .outputMode(OutputMode.Append())
        .option("checkpointLocation", "/tmp/java/checkpoint/spark")
        //.trigger(Trigger.ProcessingTime("1 second"))
        .trigger(Trigger.Once())
        .format("console")
        .start();


    try {
      query.awaitTermination(10000);
    } catch (StreamingQueryException e) {
      e.printStackTrace();
    }

  }
}
