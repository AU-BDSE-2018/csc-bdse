package ru.csc.bdse.kv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import ru.csc.bdse.util.Constants;
import ru.csc.bdse.util.Env;

import java.io.File;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.Assert.*;

/**
 * Test have to be implemented
 *
 * @author alesavin
 */
public class KeyValueApiHttpClientNonFunctionalTest {

    private static String nodeName;
    private static GenericContainer node;
    private static KeyValueApi api;

    @BeforeClass
    public static void setup() {
        nodeName = "test-node" + UUID.randomUUID().toString().substring(4);
        node = (GenericContainer) new GenericContainer(
                new ImageFromDockerfile()
                        .withFileFromFile("target/bdse-kvnode-0.0.1-SNAPSHOT.jar", new File
                                ("../bdse-kvnode/target/bdse-kvnode-0.0.1-SNAPSHOT.jar"))
                        .withFileFromClasspath("Dockerfile", "kvnode/Dockerfile"))
                .withEnv(Env.KVNODE_NAME, nodeName)
                .withExposedPorts(8080)
                .withStartupTimeout(Duration.of(30, SECONDS))
                .withFileSystemBind("/var/run/docker.sock", "/var/run/docker.sock");
        node.start();
        while (!node.isRunning()) {
            try {
                Thread.sleep(500);
            } catch (Exception e) {
                // ignore
            }
        }
        api = newKeyValueApi();
    }

    @AfterClass
    public static void clean() {
        System.out.println("Clean stage");
        api.action(nodeName, NodeAction.DOWN);
    }

    private static KeyValueApi newKeyValueApi() {
        final String baseUrl = "http://localhost:" + node.getMappedPort(8080);
        return new KeyValueApiHttpClient(baseUrl);
    }

    @Test
    public void concurrentPuts() {
        final String key = "SomeKey";
        final String value = "SomeValue";

        Thread[] threads = new Thread[100];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> api.put(key, value.getBytes()));
        }

        for (Thread th : threads) {
            th.start();
        }

        for (Thread th : threads) {
            try {
                th.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }

        final String retrievedValue = new String(api.get(key).orElse(Constants.EMPTY_BYTE_ARRAY));

        assertEquals(value, retrievedValue);

        api.delete(key);
    }

    @Test
    public void concurrentDeleteAndKeys() {
        final String key = "SomeKey";
        final String value = "SomeValue";

        final String key1 = "SomeKey1";
        final String value1 = "SomeValue1";

        // first, put values
        api.put(key, value.getBytes());
        api.put(key1, value1.getBytes());

        Thread[] threads = new Thread[100];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> api.delete(key));
        }

        for (Thread th : threads) {
            th.start();
        }

        for (Thread th : threads) {
            try {
                th.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }

        final Set<String> keys = api.getKeys("");
        assertEquals(1, keys.size());
        assertTrue(keys.contains(key1));

        final String retrievedValue = new String(api.get(key1).orElse(Constants.EMPTY_BYTE_ARRAY));
        assertEquals(value1, retrievedValue);

        api.delete(key);
        api.delete(key1);
    }

    @Test
    public void actionUpDown() {
        final String key = "SomeKey";
        final String value = "SomeValue";

        api.put(key, value.getBytes());
        api.action(nodeName, NodeAction.DOWN);
        api.action(nodeName, NodeAction.UP);

        final String retrievedValue = new String(api.get(key).orElse(Constants.EMPTY_BYTE_ARRAY));
        assertEquals(value, retrievedValue);

        api.delete(key);
    }

    @Test
    public void putWithStoppedNode() {
        final String key = "SomeKey";
        final String value = "SomeValue";

        api.action(nodeName, NodeAction.DOWN);
        api.put(key, value.getBytes());
        api.action(nodeName, NodeAction.UP);

        assertFalse(api.get(key).isPresent());

        api.delete(key);
    }

    @Test
    public void getWithStoppedNode() {
        final String key = "SomeKey";
        final String value = "SomeValue";

        api.put(key, value.getBytes());
        api.action(nodeName, NodeAction.DOWN);
        assertFalse(api.get(key).isPresent());
        api.action(nodeName, NodeAction.UP);

        api.delete(key);
    }

    @Test
    public void getKeysByPrefixWithStoppedNode() {
        //TODO test getKeysByPrefix if node/container was stopped
    }

    @Test
    public void deleteByTombstone() {
        // TODO use tombstones to mark as deleted (optional)
    }

    @Test
    public void loadMillionKeys()  {
        //TODO load too many data (optional)
    }
}


