package id.natanhp.kafka;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TCPSocketTask extends SourceTask {
    private static final Logger logger = LogManager.getLogger();
    private Socket clientSocket;
    private String topic;
    private String host;
    private int port;

    @Override
    public String version() {
        return new TCPSocketConnector().version();
    }

    @Override
    public void start(Map<String, String> map) {
        host = map.get(TCPSocketConnector.HOST);
        port = Integer.parseInt(map.get(TCPSocketConnector.PORT));
        topic = map.get(TCPSocketConnector.TOPIC);

        connectToServer();
    }

    private void connectToServer() {
        try {
            clientSocket = new Socket(host, port);
            logger.info("Socket has been initialized.");
            logger.info("Connected to " + host + ":" + port);
        } catch (IOException e) {
            logger.error("Socket initialization failed.");
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        while (clientSocket == null) {
            logger.error("Socket not initialized yet.");
            connectToServer();
        }

        InputStream inputStream;

        try {
            inputStream = clientSocket.getInputStream();
        } catch (IOException e) {
            logger.error(e.getMessage());

            recoverConnection();

            return null;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line = null;

        try {
            line = reader.readLine();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        if (line == null) {
            logger.error("The socket server is down.");

            recoverConnection();

            return null;
        }

        return List.of(new SourceRecord(
                Collections.singletonMap("socket", 0),
                Collections.singletonMap("0", 0),
                topic,
                Schema.STRING_SCHEMA,
                line
        ));
    }

    /**
     * Try to recover lost connection
     * Call {@link #stop()} to release socket
     * so the socket server can start at the same port
     * immediately.
     */
    private void recoverConnection() {
        stop();
        connectToServer();
    }

    @Override
    public void stop() {
        try {
            clientSocket.close();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }
}
