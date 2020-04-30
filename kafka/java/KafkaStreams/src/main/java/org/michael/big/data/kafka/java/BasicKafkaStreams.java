package org.michael.big.data.kafka.java;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class BasicKafkaStreams {
  public static void main(String[] args) throws Exception {
    Properties streamsConfiguration = new Properties();

    // Give the Streams application a unique name. The name must be unique in the Kafka cluster
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-streams-example");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Specify default (de)serializers for record keys and for record values.
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    StreamsBuilder builder = new StreamsBuilder();

    // Construct a KStream from the input Topic "TextLinesTopic"
    KStream<byte[], String> textLines = builder.stream("TextLinesTopic");

    // Convert to upper case
    KStream<byte[], String> uppercasedWithMapValues = textLines.mapValues(value -> value.toUpperCase());

    // Write the results to a new Kafka Topic called "UppercasedTextLinesTopic".
    uppercasedWithMapValues.to("UppercasedTextLinesTopic");

    // Run the Streams application via `start()`
    KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
    streams.start();

    // Stop the application gracefully
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
