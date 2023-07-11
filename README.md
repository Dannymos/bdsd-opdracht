The following commands can be used to register the Kafka topics:
```
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic streams-number-input

kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic streams-number-average --config cleanup.policy=compact

kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic streams-number-standard-deviation --config cleanup.policy=compact
```

To send commands from the CLI

```
docker exec -it kafka1 /bin/bash

kafka-console-producer --broker-list localhost:9092 --topic streams-number-input --property "parse.key=true" --property "key.separator=:"
```