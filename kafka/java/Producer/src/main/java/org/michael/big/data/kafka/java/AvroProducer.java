package org.michael.big.data.kafka.java;

import java.util.Calendar;
import java.util.Properties;

import com.kafkainaction.alert_status;
import com.kafkainaction.Alert;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.common.serialization.LongSerializer;


public class AvroProducer {
    public static void main(String[] args) {
        System.out.println("*** Starting AVRO Producer ***");

        // define Topic to which records are produced to
        final String topic = "avrotest";

        // Define kafkaParameter
        Properties settings = new Properties();
        settings.put(ProducerConfig.CLIENT_ID_CONFIG, "basic-producer");
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        settings.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        settings.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true);

        Producer<Long, Alert> producer = new KafkaProducer<>(settings);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("### Stopping AVRO Producer ###");
            producer.close();
        }));

        Alert alert = new Alert();
        alert.setSensorId(12345L);
        alert.setTime(Calendar.getInstance().getTimeInMillis());
        alert.setStatus(alert_status.Critical);
        System.out.println(alert.toString());

        ProducerRecord<Long, Alert> producerRecord = new ProducerRecord<>("avrotest", alert.getSensorId(), alert);

        producer.send(producerRecord);
    }
}

/* Test
./bin/kafka-avro-console-consumer \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --topic avrotest \
  --property schema.registry.url=http://localhost:8081
 */
