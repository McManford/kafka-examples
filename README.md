# Kakfa examples to be built against local build of Kafka client

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

### Run example

```shell
java -jar build/libs/kafka-examples-0.0.1-dev.jar
```
