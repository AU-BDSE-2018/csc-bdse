package ru.csc.bdse.util.containers.postgres;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
import org.jetbrains.annotations.NotNull;
import ru.csc.bdse.util.containers.ContainerManager;
import ru.csc.bdse.util.containers.ContainerStatus;

/**
 * Utils to start/stop postgres containers
 */
public final class PostgresContainerManager extends ContainerManager {

    private static final int POSTGRES_DEFAULT_PORT = 5432;
    private static final ExposedPort POSTGRES_EXPOSED_PORT = new ExposedPort(POSTGRES_DEFAULT_PORT);

    @Override
    protected void createContainer(@NotNull String containerName) {
        if (getContainerStatus(containerName) != ContainerStatus.DOES_NOT_EXIST) {
            return;
        }

        Ports portBindings = new Ports();
        portBindings.bind(POSTGRES_EXPOSED_PORT, Ports.Binding.bindPort(POSTGRES_DEFAULT_PORT));

        dockerClient.createContainerCmd("postgres:latest")
                .withPortBindings(portBindings)
                .withName(containerName)
                .withHostName("localhost")
                .withEnv("POSTGRES_PASSWORD=foobar")
                .exec();
    }

}
