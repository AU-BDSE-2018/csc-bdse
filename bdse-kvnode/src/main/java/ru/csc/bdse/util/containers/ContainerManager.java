package ru.csc.bdse.util.containers;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.core.DockerClientBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class ContainerManager {

    protected static final DockerClient dockerClient = DockerClientBuilder.getInstance().build();

    protected static ContainerStatus getContainerStatus(@NotNull String containerName) {
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

    @Nullable
    public static String getContainerIp(@NotNull String containerName) {
        final ContainerNetwork cn = dockerClient.inspectContainerCmd(containerName).exec()
                .getNetworkSettings()
                .getNetworks()
                .get("bridge");
        if (cn == null) {
            throw new IllegalStateException("DB container is not connected to docker's default bridge? O_o");
        }

        return cn.getIpAddress();
    }

    protected abstract void createContainer(@NotNull String containerName);

    /**
     * The problem is that container might be up and running, but is not ready to
     * accept connections (e.g. postgres needs some time before we can connect with
     * jdbc). Wait for it to fully initialize.
     *
     * Default implementation -- just wait for 2 seconds
     *
     * @param containerName name of the container to wait
     */
    protected void waitContainerInit(@NotNull String containerName) {
        try {
            Thread.sleep(2000);
        } catch (Exception e) {
            // ignore
        }
    }

    public boolean run(@NotNull String containerName) {
        try {
            createContainer(containerName);
            if (getContainerStatus(containerName) != ContainerStatus.RUNNING) {
                dockerClient.startContainerCmd(containerName).exec();
            }
            waitContainerInit(containerName);
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

}
