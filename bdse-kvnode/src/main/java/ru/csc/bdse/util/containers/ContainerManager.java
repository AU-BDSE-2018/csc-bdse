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

    /**
     * Create a volume for container's data and return this volumes mountpoint.
     * @param containerName name of the container
     * @return mountpoint of the volume created
     */
    @NotNull
    protected static String createVolume(@NotNull String containerName) {

        /*
         * Oh, man...
         *
         * We need to make our postgres container persistent. The logical solution is to
         * run `docker run -v /tmp/postgres_data:POSTGRES_DATA_PATH postgres`, right?
         * NO! This will create directory `/tmp/postgres_data` on _HOST_ machine, not inside
         * of our node's docker container.
         * (I feel like we need to have a serious discussion with docker developers...)
         *
         * But we want to create it inside of the node's container -- at least command `VOLUME /tmp` in
         * Dockerfile of integration tests suggests so. So how can we do that?
         *
         * I didn't find a way around to do it properly and I'm too tired already of all this docker/docker-java
         * "strangeness". So for now I thought that I would just store postgres data in host's /tmp.
         *
         * But this, of course, will not work because integration tests don't clean after
         * themselves and they always give our node the same name `node-0`, so I can not create new folder for postgres
         * data based on node's name and this old data from previous tests run remains there next time we run them.
         * This results in `getKeysByPrefix` test fail.
         *
         * So I though that maybe we just clean `/tmp/postgres_data + containerName` folder before
         * we run any tests. And guess what? We can not. You need to have root rights to do that. Thus
         * it's impossible to save postgres data in /tmp.
         *
         * Note that we also unable to use this feature (https://github.com/moby/moby/pull/19568) because
         * we need to stop/start postgres container and we can not use `--rm` flags.
         *
         * Please, smbdy kill me :(
         *
         * TL;DR;
         * I just decided to create this volumes in /tmp of host and modified tests so that they don't
         * sent default node name. See README for more info.
         *
         * I left dead code below just in case I need it later. I know that it's a bit practice
         * but I returned to that solution many times and who knows -- I might return to it again.
         */

//        final String dataVolumeName = containerName + "-volume";
//
//        dockerClient.createVolumeCmd().withName(dataVolumeName).exec();
//        final InspectVolumeResponse inspectResponse = dockerClient.inspectVolumeCmd(dataVolumeName).exec();
//        return inspectResponse.getMountpoint();

        return "/tmp/" + containerName + "-data";
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
