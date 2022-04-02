# TCP Client Kafka Connector

This is a Kafka Connector to fetch (currently only support) stream of string from a TCP Server.

## Requirements

1. Java 11
2. Maven

## Build

```bash
mvn clean package
```

## Usage

1. Config the `plugin.path` in your worker properties.

```properties
...
plugin.path=/path/to/your/executable/jar/directory
...
```

2. Create `TCPConnector.properties` with the following configurations:

```properties
name=socket-connector
connector.class=id.natanhp.kafka.TCPSocketConnector
tasks.max=1
topic=<your kafka topic>
port=<TCP Server Port>
host=<TCP Server Host>
```

3. The example of running it with standalone configuration

```bash
connect-standalone connect-standalone.properties TCPConnector.properties
```

## Contributing
Please read [CONTRIBUTING.md](CONTRIBUTING.md)