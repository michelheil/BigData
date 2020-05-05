# Instructions to use it on my local system
compile this library
export CLASSPATH=$CLASSPATH:/home/michael/GitHubRepositories/BigData/kafka/java/MirrorMakerRenameTopics/target/MirrorMakerRenameTopics-1.0.jar
confluent start local (start zookeeper and kafka-server)
kafka-topics --bootstrap-server localhost:9092 --create --replication-factor 1 --partitions 1 --topic topicToBeRenamed
kafka-topics --bootstrap-server localhost:9092 --create --replication-factor 1 --partitions 10 --topic newTopicName

```shell script
kafka-mirror-maker --consumer.config /home/michael/GitHubRepositories/BigData/kafka/java/MirrorMakerRenameTopics/src/main/resources/consumer.properties \
 --producer.config /home/michael/GitHubRepositories/BigData/kafka/java/MirrorMakerRenameTopics/src/main/resources/producer.properties \
 --num.streams 1 \
 --whitelist="topicToBeRenamed" \
 --message.handler org.michael.big.data.kafka.java.TopicRenameHandler \
 --message.handler.args "newTopicName"
```

kafka-console-producer --broker-list localhost:9092 --topic topicToBeRenamed
kafka-console-consumer --bootstrap-server localhost:9092 --from-beginning --topic newTopicName

# Answer for stackoverflow question:

As usual, there are many options to answer your question. 

My favorite option here is to make use of the **MirrorMaker** that comes with Kafka. This tool is mainly used to
replicate data from one data center to another. It is build on the assumption that the topic names stay the same in both clusters.

However, you can provide your customised `MessageHandler` that renames a topic.

```java
package org.michael.big.data.kafka.java;

import java.util.Collections;
import java.util.List;
import kafka.consumer.BaseConsumerRecord;
import kafka.tools.MirrorMaker;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * An example implementation of MirrorMakerMessageHandler that allows to rename topic.
 */
public class TopicRenameHandler implements MirrorMaker.MirrorMakerMessageHandler {
  private final String newName;

  public TopicRenameHandler(String newName) {
    this.newName = newName;
  }

  public List<ProducerRecord<byte[], byte[]>> handle(BaseConsumerRecord record) {
    return Collections.singletonList(new ProducerRecord<byte[], byte[]>(newName, record.partition(), record.key(), record.value()));
  }
}
```


I used the following dependencies in my `pom.xml` file
```xml
    <properties>
        <kafka.version>2.5.0</kafka.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>${kafka.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka_2.13</artifactId>
            <version>${kafka.version}</version>
        </dependency>
    </dependencies>
```
Compile the code above and make sure to add your class into the `CLASSPATH`
```shell script
export CLASSPATH=$CLASSPATH:/.../target/MirrorMakerRenameTopics-1.0.jar
```

Now, together with some basic `consumer.properties`
```shell script
bootstrap.servers=localhost:9092
client.id=mirror-maker-consumer
group.id=mirror-maker-rename-topic
auto.offset.reset=earliest
```
and `producer.properties`
```shell script
bootstrap.servers=localhost:9092
client.id=mirror-maker-producer
```

you can call the `kafka-mirror-maker` as below
```shell script
kafka-mirror-maker --consumer.config /path/to/consumer.properties \
 --producer.config /path/to/producer.properties \
 --num.streams 1 \
 --whitelist="topicToBeRenamed" \
 --message.handler org.michael.big.data.kafka.java.TopicRenameHandler \
 --message.handler.args "newTopicName"
```

Please note the following two caveats with this approach:
- As you are planning to change the number of partitions the ordering of the messages within the new topic might be different compared to the old topic. Messages are getting partitioned by the key in Kafka by default.
- Using the MirrorMaker will not copy your existing offsets in the old topic but rather start writing new offsets. So, there will be (almost) no relation between the offsets from the old topic to the offsets of the new topic.

# Resources
https://github.com/opencore/mirrormaker_topic_rename
https://github.com/gwenshap/kafka-examples/blob/master/MirrorMakerHandler/README.md
