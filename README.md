# openbmp-message-consumer

This tool was created to compare and validate messages OpenBMP messages sent to Kafka by OpenNMS.
 
## Usage

Requires topics to be created:
```
./bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic openbmp.parsed.base_attribute --partitions 6 --replication-factor 1
./bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic openbmp.parsed.bmp_stat --partitions 6 --replication-factor 1
./bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic openbmp.parsed.collector --partitions 6 --replication-factor 1
./bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic openbmp.parsed.peer --partitions 6 --replication-factor 1
./bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic openbmp.parsed.router --partitions 6 --replication-factor 1
./bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic openbmp.parsed.unicast_prefix --partitions 6 --replication-factor 1
```

```
feature:repo-add camel 2.25.0
feature:install camel-kafka
bundle:install -s mvn:org.apache.servicemix.bundles/org.apache.servicemix.bundles.kafka-streams/2.1.0_1
feature:install aries-blueprint
bundle:install -s mvn:com.google.guava/guava/18.0
bundle:install -s mvn:net.sf.supercsv/super-csv/2.4.0
bundle:install -s mvn:org.opennms.bundles/org.opennms.bundles.openbmp-api-message/0.1.0_1
bundle:install -s mvn:org.apache.servicemix.bundles/org.apache.servicemix.bundles.gson/2.1_1
bundle:install -s mvn:org.opennms/openbmp-message-consumer/1.0.0-SNAPSHOT
```
