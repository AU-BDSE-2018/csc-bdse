package ru.csc.bdse;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.client.StorageKeyValueApiHttpClient;
import ru.csc.bdse.kv.db.postgres.PostgresPersistentKeyValueApi;
import ru.csc.bdse.kv.replication.CoordinatorKeyValueApi;
import ru.csc.bdse.util.Env;

import javax.annotation.PreDestroy;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@SpringBootApplication
public class Application {

    static KeyValueApi storageNodeInUse;
    private static String nodeName;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @PreDestroy
    public static void stop() {
        System.out.println("stopping node " + nodeName);
        storageNodeInUse.action(nodeName, NodeAction.DOWN);
    }

    private static String randomNodeName() {
       //return "node-0";
       return "kvnode-" + UUID.randomUUID().toString().substring(4);
    }

    @Bean(name = "coordinatorNode")
    KeyValueApi coordinatorNode() {
        System.out.println("coordinatorNode init. nodeName is " + nodeName);

        final CoordinatorKeyValueApi coordinator = new CoordinatorKeyValueApi();

        final int WCL = Env.getInt(Env.WCL).orElse(1);
        final int RCL = Env.getInt(Env.RCL).orElse(1);
        final int TIMEOUT = Env.getInt(Env.TIMEOUT).orElse(1);
        final List<String> REPLICS = Env.getList(Env.REPLICS).orElse(Collections.singletonList(nodeName));

        coordinator.configure(WCL, RCL, TIMEOUT);
        REPLICS.stream()
                .map(r -> {
                    if (r.equals(nodeName)) {
                        return storageNodeInUse;
                    } else {
                        return new StorageKeyValueApiHttpClient("http://" + r + ":8080");
                    }
                })
                .forEach(coordinator::addReplica);

        return coordinator;
    }

    @Bean(name = "storageNode")
    KeyValueApi storageNode() {
        String nodeName = Env.get(Env.KVNODE_NAME).orElseGet(Application::randomNodeName);
        Application.nodeName = nodeName;
        storageNodeInUse = new PostgresPersistentKeyValueApi(nodeName);
        return storageNodeInUse;
    }

}
