package ru.csc.bdse.util.containers;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectVolumeResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.core.DockerClientBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class ContainerManager {

    protected static final DockerClient dockerClient = DockerClientBuilder.getInstance().build();

    public boolean run(@NotNull String containerName, @NotNull String networkName) {
        try {
            createContainer(containerName, networkName);
            if (getContainerStatus(containerName) != ContainerStatus.RUNNING) {
                dockerClient.startContainerCmd(containerName).exec();
            }
            waitContainerInit(containerName, networkName);
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

    /**
     * Create a volume for container's data and return this volumes mountpoint.
     * @param containerName name of the container
     * @return mountpoint of the volume created
     */
    @NotNull
    protected static String createVolume(@NotNull String containerName) {
        final String dataVolumeName = containerName + "-volume";

        dockerClient.createVolumeCmd().withName(dataVolumeName).exec();
        final InspectVolumeResponse inspectResponse = dockerClient.inspectVolumeCmd(dataVolumeName).exec();
        return inspectResponse.getMountpoint();
    }

    public static String getContainerHost(@NotNull String containerName, @Nullable String networkName) {
        if (networkName == null || networkName.equals("bridge")) {
            return getContainerIp(containerName);
        } else {
            return getContainerHostname(containerName);
        }
    }

    /**
     * Create docker network with the specified name
     * @param networkName name of the new network
     */
    public static void createNetwork(@NotNull String networkName) {
        boolean alreadyExists = dockerClient.listNetworksCmd().exec().stream().anyMatch(nw -> nw.getName().equals(networkName));
        if (alreadyExists) {
            return;
        }
        try {
            dockerClient.createNetworkCmd().withName(networkName).exec();
        } catch (Exception e) {
            // Just print it for now
            System.err.println("Caught exception " + e + " while creating docker network '" + networkName + "'");
            e.printStackTrace();
        }
    }

    protected abstract void createContainer(@NotNull String containerName, @NotNull String networkName);

    /**
     * The problem is that container might be up and running, but is not ready to
     * accept connections (e.g. postgres needs some time before we can connect with
     * jdbc). Wait for it to fully initialize.
     *
     * Default implementation -- just wait for 2 seconds
     *
     * @param containerName name of the container to wait
     */
    protected void waitContainerInit(@NotNull String containerName, @NotNull String networkName) {
        try {
            Thread.sleep(2000);
        } catch (Exception e) {
            // ignore
        }
    }
//
//    private static void connectContainer(@NotNull String containerName, @NotNull String networkName) {
//        dockerClient
//                .connectToNetworkCmd()
//                .withContainerId(containerName)
//                .withNetworkId(networkName)
//                .exec();
//    }

    private static String getContainerIp(@NotNull String containerName) {
        final ContainerNetwork cn = dockerClient.inspectContainerCmd(containerName).exec()
                .getNetworkSettings()
                .getNetworks()
                .get("bridge");
        if (cn == null) {
            throw new IllegalStateException("DB container is not connected to docker's default bridge");
        }

        return cn.getIpAddress();
    }

    private static String getContainerHostname(@NotNull String containerName) {
        return dockerClient.inspectContainerCmd(containerName).exec()
                .getConfig().getHostName();
    }

}
