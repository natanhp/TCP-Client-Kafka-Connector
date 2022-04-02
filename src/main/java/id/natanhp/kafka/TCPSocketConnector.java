package id.natanhp.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class TCPSocketConnector extends SourceConnector {
    private static final Logger logger = LogManager.getLogger();

    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String TOPIC = "topic";

    private String host;
    private String port;
    private String topic;

    @Override
    public void start(Map<String, String> map) {
        host = map.get(HOST);
        validateConfiguration(host, HOST);

        port = map.get(PORT);
        validateConfiguration(port, PORT);

        topic = map.get(TOPIC);
        validateConfiguration(topic, TOPIC);
    }

    private void validateConfiguration(String config, String key) {
        if (config == null || config.isEmpty()) {
            logger.fatal("Missing " + key);
            throw new ConnectException(key + " is required");
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TCPSocketTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        return List.of(Map.ofEntries(
                Map.entry(HOST, host),
                Map.entry(PORT, port),
                Map.entry(TOPIC, topic)
        ));
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(HOST, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "TCP Server Host")
                .define(PORT, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "TCP Server Port")
                .define(TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Kafka Topic");
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
