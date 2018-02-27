package ru.csc.bdse.util.containers.postgres;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
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

    @Override
    protected void waitContainerInit(@NotNull String containerName) {
        // TODO I'm to lazy and tired to write this in a normal way, thus this solution for now

        /*
         * for some reason 'until' shell command does not work here, so using ugly while
         */
        final String CHEAT_COMMAND = "while (( 1 == 1 )); do docker run --rm --link " + containerName
                + ":pg postgres:latest pg_isready -U postgres -h pg; if (( $? == 0 )); then break; fi; done";

        try {
            ProcessBuilder builder = new ProcessBuilder();
            builder.redirectErrorStream(true);
            builder.command("/bin/bash", "-c", CHEAT_COMMAND);
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
