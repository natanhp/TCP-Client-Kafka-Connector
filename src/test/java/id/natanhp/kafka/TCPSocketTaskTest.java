package id.natanhp.kafka;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class TCPSocketTaskTest {
    static TCPSocketTask tcpSocketTask;

    @BeforeAll
    static void setup() {
        tcpSocketTask = new TCPSocketTask();
        tcpSocketTask.start(Map.ofEntries(
                Map.entry(TCPSocketConnector.HOST, "localhost"),
                Map.entry(TCPSocketConnector.PORT, "10000"),
                Map.entry(TCPSocketConnector.TOPIC, "socket_topic")
        ));
    }

//    @Test
    void testPoll() {
        try {
            List<SourceRecord> test = tcpSocketTask.poll();

            test.forEach(t -> System.out.println(t.toString()));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @AfterAll
    static void stop() {
        tcpSocketTask.stop();
    }
}
