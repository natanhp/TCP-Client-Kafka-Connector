package id.natanhp.kafka;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TCPSocketTaskTest {
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private PrintWriter out;
    private int serverPort;
    private TCPSocketTask tcpSocketTask;

    @BeforeEach
    void init() {
        try {
            serverSocket = new ServerSocket(0);
            serverPort = serverSocket.getLocalPort();
            new Thread(() -> {
                try {
                    clientSocket = serverSocket.accept();
                    out = new PrintWriter(clientSocket.getOutputStream(), true);
                    out.println("A");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        } catch (IOException e) {
            e.printStackTrace();
        }

        tcpSocketTask = new TCPSocketTask();

        tcpSocketTask.start(Map.ofEntries(
                Map.entry(TCPSocketConnector.HOST, "localhost"),
                Map.entry(TCPSocketConnector.PORT, String.valueOf(serverPort)),
                Map.entry(TCPSocketConnector.TOPIC, "topic")
        ));
    }

    @Test
    void testReceivingStream() {
        try {
            assertEquals(tcpSocketTask.poll().get(0).value(), "A");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @AfterEach
    void tearDown() {
        tcpSocketTask.stop();
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
