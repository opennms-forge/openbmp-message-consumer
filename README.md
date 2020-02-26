# openbmp-message-consumer

This tool was created to compare and validate messages OpenBMP messages sent to Kafka by OpenNMS.
 
## Usage

```
bundle:watch *
feature:install kafka-streams/2.3.0
bundle:install -s 'wrap:mvn:org.apache.kafka/kafka-streams/2.3.0$Bundle-Version=2.3.0&Export-Package=*;-noimport:=true:version="2.3.0"'
bundle:install -s mvn:org.opennms/openbmp-message-consumer/1.0.0-SNAPSHOT
```


Requires topics to be created:
```
./bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic opennms.openbmp.parsed.base_attribute --partitions 6 --replication-factor 1
./bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic opennms.openbmp.parsed.bmp_stat --partitions 6 --replication-factor 1
./bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic opennms.openbmp.parsed.collector --partitions 6 --replication-factor 1
./bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic opennms.openbmp.parsed.peer --partitions 6 --replication-factor 1
./bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic opennms.openbmp.parsed.router --partitions 6 --replication-factor 1
./bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic opennms.openbmp.parsed.unicast_prefix --partitions 6 --replication-factor 1
```
