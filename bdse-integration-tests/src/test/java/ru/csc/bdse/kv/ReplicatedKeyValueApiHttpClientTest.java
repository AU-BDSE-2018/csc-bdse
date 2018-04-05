package ru.csc.bdse.kv;

import org.junit.BeforeClass;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import ru.csc.bdse.kv.client.ControllerKeyValueApiHttpClient;
import ru.csc.bdse.kv.client.ReplicatedKeyValueApiHttpClient;
import ru.csc.bdse.util.Env;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * @author semkagtn
 */
public class ReplicatedKeyValueApiHttpClientTest extends AbstractKeyValueApiTest {

    private static GenericContainer node;
    private static List<String> nodeUrls = new ArrayList<>();

    @BeforeClass
    public static void init() {
        node = (GenericContainer) new GenericContainer(
                new ImageFromDockerfile()
                        .withFileFromFile("target/bdse-kvnode-0.0.1-SNAPSHOT.jar", new File
                                ("../bdse-kvnode/target/bdse-kvnode-0.0.1-SNAPSHOT.jar"))
                        .withFileFromClasspath("Dockerfile", "kvnode/Dockerfile"))
                .withEnv(Env.KVNODE_NAME, "node-0")
                .withExposedPorts(8080)
                .withStartupTimeout(Duration.of(30, SECONDS))
                .withFileSystemBind("/var/run/docker.sock", "/var/run/docker.sock");
        node.start();

        final String nodeUrl = "http://" + node.getContainerIpAddress() + ":" + node.getMappedPort(8080);
        nodeUrls.add(nodeUrl);
        final ControllerKeyValueApiHttpClient nodeCoordinator = new ControllerKeyValueApiHttpClient(nodeUrl);
        nodeCoordinator.configure(1, 1, 1);
        nodeCoordinator.addReplica("-");
    }

    @Override
    protected KeyValueApi newKeyValueApi() {
        return new ReplicatedKeyValueApiHttpClient(nodeUrls);
    }

}
