https://docs.confluent.io/current/connect/userguide.html#connect-installing-plugins

start hbase (hBaseServer.md)
start kafka

mvn clean package

Open target/hBaseSinkConnector-0.1-package in Terminal
cp -r share /home/michael/confluent/confluent-5.5.0/share/confluent-hub-components/michael-hbase-sink

kafka-topics --bootstrap-server localhost:9092 --create --replication-factor 1 --partitions 1 --topic myTopic
kafka-console-producer --broker-list localhost:9092 --topic myTopic

connect-standalone /home/michael/confluent/confluent-5.5.0/share/confluent-hub-components/michael-hbase-sink/etc/connect-standalone.properties /home/michael/confluent/confluent-5.5.0/share/confluent-hub-components/michael-hbase-sink/etc/hbase-sink-connector.properties