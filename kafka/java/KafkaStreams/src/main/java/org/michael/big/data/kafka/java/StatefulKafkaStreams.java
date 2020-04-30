package org.michael.big.data.kafka.java;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class StatefulKafkaStreams {
  public static void main(String[] args) throws Exception {
    Properties streamsConfiguration = new Properties();

    // Give the Streams application a unique name. The name must be unique in the Kafka cluster
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "StatelessKafkaStreams");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Specify default (de)serializers for record keys and for record values.
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    StreamsBuilder builder = new StreamsBuilder();

    // Construct a KStream from the input Topic "TextLinesTopic"
    KStream<String, String> textLines = builder.stream("TextLinesTopic2");

    KTable<String, Long> wordCounts = textLines
        .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
        .groupBy((key, word) -> word) // Count the occurrences of each word (record key).
        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));

    // Write the `KTable<String, Long>` to an output Topic
    wordCounts.toStream().to("WordsWithCountsTopic2", Produced.with(Serdes.String(), Serdes.Long()));

    // Run the Streams application via `start()`.
    KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
    streams.start();

    // Stop the application gracefully
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
