In this module, I am trying to play around with Spark's StreamingQueryListener.

As a use case I plan to manually commit offset of a Kafka Topic within a Structured Stream of Spark.


Idea:
* Start Kafka on localhost (incl. Zookeeper)
* Create Topic with one partition
* Run Spark Structured Streaming subscribing to this topic
* Observe internal topic __consumer_offset and figure out what the group.id is and if offsets are getting committed
(based on documentation it is not possible to set group.id in Structured Streaming and also not possible
to enable auto.offset.reset as Kafka properties.)
* Create a StreamingQueryListener that manually commits offsets to Kafka during `onQueryProcess`
* Observe internal topic __consumer_offset and figure out what the group.id is and if offsets are getting committed


start Zookeeper
start Kafka
create Kafka topics
./kafka/current/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic testingKafkaProducer
./kafka/current/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic testingKafkaProducerOut
sbt package
cd /home/michael/spark/spark-2.4.0-bin-hadoop2.7
./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --master local[1] --class org.michael.big.data.sparkStreamingQueryListener.ListenerBootstrap /home/michael/GitHubRepositories/BigData/sparkStreamingQueryListener/target/scala-2.11/sparkstreamingquerylistener_2.11-0.1.jar

./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-10_2.11:2.4.5 --master local[1] --class org.michael.big.data.sparkStreamingQueryListener.ListenerBootstrap /home/michael/GitHubRepositories/BigData/sparkStreamingQueryListener/target/scala-2.11/sparkstreamingquerylistener_2.11-0.1.jar