package org.michael.big.data.kafka.java;

import com.kafkainaction.Alert;
import com.kafkainaction.alert_status;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.Calendar;
import java.util.Properties;


public class AvroProducerDateTimes {
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

        Producer<Long, DateTimes> producer = new KafkaProducer<>(settings);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("### Stopping AVRO Producer ###");
            producer.close();
        }));

        Integer rawInt = 5000;
        Long rawLong = 12345L;

        DateTimes dateTimes = new DateTimes();
        dateTimes.setRawInt(rawInt);
        dateTimes.setRawLong(rawLong);
        dateTimes.setDate(rawInt);
        dateTimes.setTimeMilli(rawInt);
        dateTimes.setTimeMicro(1000*Calendar.getInstance().getTimeInMillis());
        dateTimes.setTimestampMilli(Calendar.getInstance().getTimeInMillis());
        dateTimes.setTimestampMicro(Calendar.getInstance().getTimeInMillis()*1000);
        System.out.println(dateTimes.toString());

        ProducerRecord<Long, DateTimes> producerRecord = new ProducerRecord<>("avrotest", 0L, dateTimes);

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
