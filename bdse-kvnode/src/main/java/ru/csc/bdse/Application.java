package ru.csc.bdse;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.db.postgres.PostgresPersistentKeyValueApi;
import ru.csc.bdse.util.Env;

import javax.annotation.PreDestroy;
import java.util.UUID;

@SpringBootApplication
public class Application {

    private static KeyValueApi nodeInUse;
    private static String nodeName;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @PreDestroy
    public static void stop() {
        System.out.println("stopping node " + nodeName);
        nodeInUse.action(nodeName, NodeAction.DOWN);
    }

    private static String randomNodeName() {
        return "kvnode-" + UUID.randomUUID().toString().substring(4);
    }

    @Bean
    KeyValueApi node() {
        String nodeName = Env.get(Env.KVNODE_NAME).orElseGet(Application::randomNodeName);
        Application.nodeName = nodeName;
        nodeInUse = new PostgresPersistentKeyValueApi(nodeName);
        return nodeInUse;
    }
}
