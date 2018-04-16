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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoUnit.SECONDS;

public class RF3WCL2RCL3Test {

    private static final List<GenericContainer> nodes = new ArrayList<>();
    private static final int RF = 3;
    private static final String WCL = "2";
    private static final String RCL = "3";
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

        // all nodes are up
        String key = Random.nextKey();
        byte[] value = Random.nextValue();

        Optional<byte[]> oldValue = api.get(key);
        softAssert.assertThat(oldValue.isPresent()).as("old value").isFalse();

        api.put(key, value);
        byte[] newValue = api.get(key).orElse(ru.csc.bdse.util.Constants.EMPTY_BYTE_ARRAY);
        softAssert.assertThat(newValue).as("new value").isEqualTo(value);

        // put one node down. We should be able to put values still
        api.action("node-0", NodeAction.DOWN);

        String key1 = Random.nextKey();
        byte[] value1 = Random.nextValue();
        api.put(key1, value1);

        String key2 = Random.nextKey();
        byte[] value2 = Random.nextValue();
        api.put(key2, value2);

        // we should fail to get
        boolean isException = false;

        try {
            api.get(key1);
        } catch (Exception e) {
            isException = e.getMessage().contains("Failed to get");
        }

        softAssert.assertThat(isException).as("get exception").isEqualTo(true);

        // bring node back up, now we should be able to get
        api.action("node-0", NodeAction.UP);

        newValue = api.get(key).orElse(ru.csc.bdse.util.Constants.EMPTY_BYTE_ARRAY);
        softAssert.assertThat(newValue).as("new value key").isEqualTo(value);

        newValue = api.get(key1).orElse(ru.csc.bdse.util.Constants.EMPTY_BYTE_ARRAY);
        softAssert.assertThat(newValue).as("new value key1").isEqualTo(value1);

        newValue = api.get(key2).orElse(ru.csc.bdse.util.Constants.EMPTY_BYTE_ARRAY);
        softAssert.assertThat(newValue).as("new value key2").isEqualTo(value2);

        softAssert.assertAll();
    }

}
