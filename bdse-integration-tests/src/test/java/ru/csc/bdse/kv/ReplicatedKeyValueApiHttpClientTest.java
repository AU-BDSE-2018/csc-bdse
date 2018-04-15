package ru.csc.bdse.kv;

import org.junit.BeforeClass;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import ru.csc.bdse.kv.client.ReplicatedKeyValueApiHttpClient;
import ru.csc.bdse.util.Env;

import java.io.File;
import java.time.Duration;
import java.util.Collections;

import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * @author semkagtn
 */
public class ReplicatedKeyValueApiHttpClientTest extends AbstractKeyValueApiTest {

    private static GenericContainer node;

    @BeforeClass
    public static void init() {
        node = (GenericContainer) new GenericContainer(
                new ImageFromDockerfile()
                        .withFileFromFile("target/bdse-kvnode-0.0.1-SNAPSHOT.jar", new File
                                ("../bdse-kvnode/target/bdse-kvnode-0.0.1-SNAPSHOT.jar"))
                        .withFileFromClasspath("Dockerfile", "kvnode/Dockerfile"))
                .withEnv(Env.KVNODE_NAME, "node-0")
                .withEnv(Env.NETWORK_NAME, "test-network")
                .withEnv(Env.WCL, "1")
                .withEnv(Env.RCL, "1")
                .withEnv(Env.TIMEOUT, "1")
                .withEnv(Env.REPLICS, "node-0")
                .withNetworkAliases("node-0")
                .withNetworkMode("test-network")
                .withExposedPorts(8080)
                .withStartupTimeout(Duration.of(30, SECONDS))
                .withFileSystemBind("/var/run/docker.sock", "/var/run/docker.sock");
        node.start();
    }

    @Override
    protected KeyValueApi newKeyValueApi() {
        return new ReplicatedKeyValueApiHttpClient(Collections.singletonList("http://localhost:" + node.getMappedPort(8080)));
    }

}
