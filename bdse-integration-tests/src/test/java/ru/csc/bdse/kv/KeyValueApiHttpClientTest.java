package ru.csc.bdse.kv;

import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import ru.csc.bdse.kv.client.ControllerKeyValueApiHttpClient;
import ru.csc.bdse.util.Env;

import java.io.File;
import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * @author semkagtn
 */
public class KeyValueApiHttpClientTest extends AbstractKeyValueApiTest {

    public static GenericContainer node;

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyValueApiHttpClientTest.class);
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
        final ControllerKeyValueApiHttpClient nodeCoordinator = new ControllerKeyValueApiHttpClient(nodeUrl);
        nodeCoordinator.configure(1, 1, 1);
        nodeCoordinator.addReplica("-");
    }
//
//    @ClassRule
//    public static final GenericContainer node = (GenericContainer) new GenericContainer(
//            new ImageFromDockerfile()
//                    .withFileFromFile("target/bdse-kvnode-0.0.1-SNAPSHOT.jar", new File
//                            ("../bdse-kvnode/target/bdse-kvnode-0.0.1-SNAPSHOT.jar"))
//                    .withFileFromClasspath("Dockerfile", "kvnode/Dockerfile"))
//            .withEnv(Env.KVNODE_NAME, "node-0")
//            .withExposedPorts(8080)
//            .withStartupTimeout(Duration.of(30, SECONDS))
//            .withFileSystemBind("/var/run/docker.sock", "/var/run/docker.sock");

    @Override
    protected KeyValueApi newKeyValueApi() {
        final String baseUrl = "http://localhost:" + node.getMappedPort(8080);
        return new ControllerKeyValueApiHttpClient(baseUrl);
    }
}
