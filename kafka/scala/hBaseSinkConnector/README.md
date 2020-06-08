https://docs.confluent.io/current/connect/userguide.html#connect-installing-plugins

## Hot to run on my local machine
### Starting HDFS/HBase
(als user 'michael')
cd ~/GitHubRepositories/ToolCommands/hadoop
bash start.sh

### Starting Kafka
cd /home/michael/kafka/current
bash bin/kafka-server-start.sh config/server.properties

### Create Java Archive (JAR) of connector
mvn clean package

### Copy Connector into `plugin.path` of Connect
Open target/hBaseSinkConnector-0.1-package in Terminal
rm -rf /home/michael/confluent/confluent-5.5.0/share/confluent-hub-components/michael-hbase-sink
cp -r share /home/michael/confluent/confluent-5.5.0/share/confluent-hub-components/michael-hbase-sink

### Create InputTopic and 
kafka-topics --bootstrap-server localhost:9092 --create --replication-factor 1 --partitions 1 --topic myTopic2

### Ensure that user `michael` has access to write into HBase
sudo su - hadoop hbase shell
grant 'michael', 'RWCA'

### Start Kafka Connect in standalone
connect-standalone /home/michael/confluent/confluent-5.5.0/share/confluent-hub-components/michael-hbase-sink/etc/connect-standalone.properties /home/michael/confluent/confluent-5.5.0/share/confluent-hub-components/michael-hbase-sink/etc/hbase-sink-connector.properties

### Produce data into Input topic
kafka-console-producer --broker-list localhost:9092 --topic myTopic2 --property "parse.key=true" --property "key.separator=:::"

### Check if data is flowing into HBase
scan 'myTable'