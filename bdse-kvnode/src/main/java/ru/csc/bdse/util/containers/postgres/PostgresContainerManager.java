package ru.csc.bdse.util.containers.postgres;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.core.DockerClientBuilder;
import org.jetbrains.annotations.NotNull;

/**
 * Utils to start/stop postgres containers
 */
public class PostgresContainerManager {

    private static final int POSTGRES_DEFAULT_PORT = 5432;
    private static final ExposedPort POSTGRES_EXPOSED_PORT = new ExposedPort(POSTGRES_DEFAULT_PORT);

    private static final DockerClient dockerClient = DockerClientBuilder.getInstance().build();

    private static ContainerStatus getContainerStatus(@NotNull String containerName) {
        String containerStatus;

        /*
         * I haven't found how to check for existence without try/catch
         */
        try {
            containerStatus = dockerClient.inspectContainerCmd(containerName).exec()
                    .getState()
                    .getStatus();
        } catch (NotFoundException e) {
            return ContainerStatus.DOES_NOT_EXIST;
        }

        if ("running".equals(containerStatus)) {
            return ContainerStatus.RUNNING;
        } else if ("exited".equals(containerStatus)) {
            return ContainerStatus.PAUSED;
        } else if ("created".equals(containerStatus)) {
            return ContainerStatus.PAUSED;
        } else {
            throw new IllegalStateException("Container status is " + containerStatus);
        }
    }

    private static void createContainer(@NotNull String containerName) {
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

    public static boolean run(@NotNull String containerName) {
        try {
            createContainer(containerName);
            if (getContainerStatus(containerName) != ContainerStatus.RUNNING) {
                dockerClient.startContainerCmd(containerName).exec();
            }
            return true;
        } catch (Exception e) {
            System.err.println("Failed to create postgres container." + e);
            e.printStackTrace();
            return false;
        }
    }

    public static boolean stop(@NotNull String containerName) {
        dockerClient.stopContainerCmd(containerName).exec();
        return true;
    }

    private enum ContainerStatus {
        RUNNING,
        PAUSED,
        DOES_NOT_EXIST
    }

}
