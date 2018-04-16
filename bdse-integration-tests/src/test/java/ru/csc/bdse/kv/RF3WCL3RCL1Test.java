package ru.csc.bdse.kv;

import com.github.dockerjava.api.command.CreateContainerCmd;
import org.assertj.core.api.SoftAssertions;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.images.builder.ImageFromDockerfile;
import ru.csc.bdse.Constants;
import ru.csc.bdse.kv.client.ReplicatedKeyValueApiHttpClient;
import ru.csc.bdse.util.Env;
import ru.csc.bdse.util.Random;
import ru.csc.bdse.util.containers.ContainerManager;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class RF3WCL3RCL1Test {

    private static final List<GenericContainer> nodes = new ArrayList<>();
    private static final int RF = 3;
    private static final String WCL = "3";
    private static final String RCL = "1";
    private static final String TIMEOUT = "5";
    private static final String REPLICS = "node-0,node-1,node-2";
    private static KeyValueApi api;

    private static GenericContainer initOneNode(@NotNull String nodeName) {
        GenericContainer node = (GenericContainer) new GenericContainer(
                new ImageFromDockerfile()
                        .withFileFromFile("target/bdse-kvnode-0.0.1-SNAPSHOT.jar", new File
                                ("../bdse-kvnode/target/bdse-kvnode-0.0.1-SNAPSHOT.jar"))
                        .withFileFromClasspath("Dockerfile", "kvnode/Dockerfile"))
                .withEnv(Env.KVNODE_NAME, nodeName)
                .withEnv(Env.NETWORK_NAME, Constants.TEST_NETWORK)
                .withEnv(Env.WCL, WCL)
                .withEnv(Env.RCL, RCL)
                .withEnv(Env.TIMEOUT, TIMEOUT)
                .withEnv(Env.REPLICS, REPLICS)
                .withNetworkMode(Constants.TEST_NETWORK)
                // for some reason, direct invocation of .withNetworkAliases doesn't work
                .withCreateContainerCmdModifier(cmd -> ((CreateContainerCmd)cmd).withAliases(nodeName))
                .withExposedPorts(8080)
                .withStartupTimeout(Duration.of(30, SECONDS))
                .withFileSystemBind("/var/run/docker.sock", "/var/run/docker.sock");
        System.out.println("Starting node '" + nodeName + "'");
        node.withLogConsumer(new Consumer<OutputFrame>() {
            @Override
            public void accept(OutputFrame outputFrame) {
                System.err.print(outputFrame.getUtf8String());
            }
        });
        node.start();
        return node;
    }

    @BeforeClass
    public static void init() {
        ContainerManager.createNetwork(Constants.TEST_NETWORK);
        for (int i = 0; i < RF; i++) {
            nodes.add(initOneNode("node-" + i));
        }
        api = newKeyValueApi();
    }

    private static KeyValueApi newKeyValueApi() {
        List<String> replics = nodes.stream().map(n -> "http://localhost:" + n.getMappedPort(8080))
                .collect(Collectors.toList());
        return new ReplicatedKeyValueApiHttpClient(replics);
    }

    @Test
    public void createValue() {
        SoftAssertions softAssert = new SoftAssertions();

        String key = Random.nextKey();
        byte[] value = Random.nextValue();

        Optional<byte[]> oldValue = api.get(key);
        softAssert.assertThat(oldValue.isPresent()).as("old value").isFalse();

        api.put(key, value);
        byte[] newValue = api.get(key).orElse(ru.csc.bdse.util.Constants.EMPTY_BYTE_ARRAY);
        softAssert.assertThat(newValue).as("new value").isEqualTo(value);

        // ok, now stop one of the nodes
        api.action("node-1", NodeAction.DOWN);

        // as RCL = 1, previous value should be the same
        newValue = api.get(key).orElse(ru.csc.bdse.util.Constants.EMPTY_BYTE_ARRAY);
        softAssert.assertThat(newValue).as("new value with stopped node").isEqualTo(value);

        // but we should fail to put a new one
        String key1 = Random.nextKey();
        byte[] value1 = Random.nextValue();
        boolean isException = false;

        try {
            api.put(key1, value1);
        } catch (Exception e) {
            isException = e.getMessage().contains("Failed to put");
        }

        softAssert.assertThat(isException).as("put exception").isEqualTo(true);

        // UP node again, after that we should be able to put a new value
        api.action("node-1", NodeAction.UP);
        api.put(key1, value1);
        newValue = api.get(key1).orElse(ru.csc.bdse.util.Constants.EMPTY_BYTE_ARRAY);
        softAssert.assertThat(newValue).as("new value after UP").isEqualTo(value1);

        softAssert.assertAll();
    }

    @Test
    public void getKeysTest() {
        final String key1 = "key1";
        final String value1 = "value1";

        final String key2 = "key2";
        final String value2 = "value2";

        api.put(key1, value1.getBytes());
        api.put(key2, value2.getBytes());

        Set<String> answer = new HashSet<>(Arrays.asList(key1, key2));
        Set<String> getRes = api.getKeys("key");

        assertEquals(answer, getRes);

        api.action("node-0", NodeAction.DOWN);
        api.action("node-2", NodeAction.DOWN);

        getRes = api.getKeys("key");

        assertEquals(answer, getRes);

        api.action("node-0", NodeAction.UP);
        api.action("node-2", NodeAction.UP);
    }

}
