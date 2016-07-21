# Kakfa examples that can be built against local build of Kafka clients

## How to build

### Install gradle

### Download Kafka repository and build kafka client

```shell
git clone https://github.com/apache/kafka
cd kafka
gradle clients:jar
ls ./clients/build/libs/kafka-clients-0.10.1.0-SNAPSHOT.jar
```

### Build this examples

Edit build.gradle and change kafka-clients dependency version if necessary

Build jar

```shell
gradle clean jar
```

### Start Zookeeper and Kafka servers

```shell
./bin/zookeeper-server-start.sh config/zookeeper.properties
./bin/kafka-server-start.sh config/server.properties
```

### Run examples

```shell
./bin/ConsumerProducerDemo1.sh async 5
./bin/ConsumerProducerDemo2.sh sync 10
```
