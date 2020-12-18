import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Main {

  public static void main(String[] args) throws InterruptedException {
    SparkConf sparkConf = new SparkConf();
    sparkConf.setAppName("kafakTest2");

    JavaStreamingContext streamingContext = new JavaStreamingContext(
        sparkConf, Durations.seconds(10));

    Map<String, Object> kafkaParams = new HashMap<String, Object>();
    kafkaParams.put("bootstrap.servers", "kafka.kafka:9092");
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("group.id", "spark_group1");
    kafkaParams.put("auto.offset.reset", "earliest");
    kafkaParams.put("enable.auto.commit", true);
    kafkaParams.put("partition.assignment.strategy", "range");

    Map<String, Integer> topics = new HashMap<String, Integer>();
    topics.put("dev_parcels_belt_scan", 1);

    Properties props = new Properties();
    props.put("bootstrap.servers", "kafka.kafka:9092");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("partition.assignment.strategy", "range");

    JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(streamingContext, "kafka-zookeeper.kafka:2181", "group1", topics);

    lines.foreachRDD(rdd -> {
      rdd.foreachPartition(iterator -> {
            KafkaProducer producer = new KafkaProducer<String,String>(props);
            while (iterator.hasNext()) {
              Tuple2<String, String> next = iterator.next();
              System.out.println(next._1() + " --> " + next._2());
              ProducerRecord record = new ProducerRecord<String,String>("spark","key",next._2());
              producer.send(record);
            }
          }
      );
    });
}
