package com.example.natspractice.logic;

import io.nats.client.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

@Slf4j
public class SimplePubSubTest {

    private Connection genConnection() throws IOException, InterruptedException {
        Connection connection = Nats.connect("nats://localhost:4222");
        log.debug("connected.... {}", connection);
        return connection;
    }

    @Test
    public void simpleConnection() throws IOException, InterruptedException {
        Connection connection = genConnection();

        connection.close();
    }

    @Test
    public void simplePubSubTest() throws IOException, InterruptedException, TimeoutException {
        Connection subConn = genConnection();

        CountDownLatch latch = new CountDownLatch(1);
        Dispatcher dispatcher = subConn.createDispatcher(
                message -> {
                    String msgStr = new String(message.getData(), StandardCharsets.UTF_8);
                    log.debug("sub...{}", msgStr);
                    latch.countDown();
                }
        );
        dispatcher.subscribe("foo");

        // pub start
        Connection pubConn = genConnection();

        pubConn.publish("foo", "Hello World".getBytes(StandardCharsets.UTF_8));
        pubConn.flush(Duration.ZERO);

        log.debug("published...");

        pubConn.close();
        // pub end

        latch.await();

        subConn.close();
    }
}
