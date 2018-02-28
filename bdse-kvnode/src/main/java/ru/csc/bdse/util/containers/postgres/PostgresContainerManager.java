package ru.csc.bdse.util.containers.postgres;

import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Volume;
import org.jetbrains.annotations.NotNull;
import ru.csc.bdse.util.containers.ContainerManager;
import ru.csc.bdse.util.containers.ContainerStatus;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Utils to start/stop postgres containers
 */
public final class PostgresContainerManager extends ContainerManager {

    private static final String POSTGRES_IMAGE_NAME = "postgres:latest";
    private static final String POSTGRES_ENV = "POSTGRES_PASSWORD=foobar";
    private static final String POSTGRES_DATA_PATH = "/var/lib/postgresql/data"; // path inside of a container
    /*
     * I don't fully understand the task so for now let it be 1 volume for all nodes.
     */
    // private static final String POSTGRES_DATA_VOLUME_NAME = "bdse-postgres-data";

    @Override
    protected void createContainer(@NotNull String containerName) {
        if (getContainerStatus(containerName) != ContainerStatus.DOES_NOT_EXIST) {
            return;
        }

        /*
         * Omg this docker-java library is a total shit in terms of running
         * containers with volumes. Thus this ton of ugly code.
         * I feel like I need to make them a pull-request solving it..
         * https://github.com/docker-java/docker-java/issues/523
         *
         * The code below is commented because I don't fully understand the task.
         * This code makes the node TRULY persistent -- even after node container AND postgres
         * container are closed we still can retrieve all the data. However, tests expect that
         * we always run new clean container -- they don't clean after themselves and still see
         * the data they put there last time I ran them.
         *
         * I left this dead code just in case I need it eventually.
         */
//        dockerClient.createVolumeCmd().withName(POSTGRES_DATA_VOLUME_NAME).exec();
//        final InspectVolumeResponse inspectVolumeResponse = dockerClient.inspectVolumeCmd(POSTGRES_DATA_VOLUME_NAME).exec();
//        final String volumeMountpoint = inspectVolumeResponse.getMountpoint();

        final String volumeMountpoint = "/tmp/postgres_data";
        final Volume postgresData = new Volume(POSTGRES_DATA_PATH);

        dockerClient.createContainerCmd(POSTGRES_IMAGE_NAME)
                .withName(containerName)
                .withBinds(new Bind(volumeMountpoint, postgresData))
                .withEnv(POSTGRES_ENV)
                .withLinks()
                .exec();
    }

    @Override
    protected void waitContainerInit(@NotNull String containerName) {
        // TODO I'm to lazy and tired to write this in a normal way, thus this solution for now

        final String CHEAT_COMMAND = "until docker run --rm --link " + containerName
                + ":pg postgres:latest pg_isready -U postgres -h pg; do sleep 1; done";

        try {
            ProcessBuilder builder = new ProcessBuilder();
            builder.redirectErrorStream(true);
            builder.command("/bin/sh", "-c", CHEAT_COMMAND);
            Process process = builder.start();
            StreamGobbler streamGobbler =
                    new StreamGobbler(process.getInputStream(), System.out::println);
            Executors.newSingleThreadExecutor().submit(streamGobbler);
            process.waitFor();
        } catch (Exception e) {
            System.err.println("Exception while waiting for postgres container: " + e);
            super.waitContainerInit(containerName);
        }
    }

    private static class StreamGobbler implements Runnable {
        private InputStream inputStream;
        private Consumer<String> consumer;

        private StreamGobbler(InputStream inputStream, Consumer<String> consumer) {
            this.inputStream = inputStream;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            new BufferedReader(new InputStreamReader(inputStream)).lines()
                    .forEach(consumer);
        }
    }

}
