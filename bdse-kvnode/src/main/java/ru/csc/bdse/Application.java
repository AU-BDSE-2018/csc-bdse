package ru.csc.bdse;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.annotation.Order;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.client.StorageKeyValueApiHttpClient;
import ru.csc.bdse.kv.db.postgres.PostgresPersistentKeyValueApi;
import ru.csc.bdse.kv.replication.CoordinatorKeyValueApi;
import ru.csc.bdse.util.Env;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@SpringBootApplication
public class Application {

    private static int WCL, RCL;
    private static int timeout;
    private static KeyValueApi storageNodeInUse;
    private static String nodeName;

    private static List<String> baseUrls;

    public static void main(String[] args) {
        if (args.length > 0 && args[0].equals("REPLICATED")) {
            WCL = Integer.parseInt(args[1]);
            RCL = Integer.parseInt(args[2]);
            timeout = Integer.parseInt(args[3]);
            baseUrls = Arrays.asList(Arrays.copyOfRange(args, 4, args.length));
        }

        SpringApplication.run(Application.class, args);
    }

    @PreDestroy
    public static void stop() {
        System.out.println("stopping node " + nodeName);
        storageNodeInUse.action(nodeName, NodeAction.DOWN);
    }

    private static String randomNodeName() {
        return "kvnode-" + UUID.randomUUID().toString().substring(4);
    }

    @Order(1) // it is important to create this Bean _AFTER_ storageNode, so it should have higher order
    @Bean(name = "coordinatorNode")
    KeyValueApi coordinatorNode() {
        final List<KeyValueApi> apis = new ArrayList<>();
        for (String baseUrl: baseUrls) {
            if (baseUrl.equals("-")) { // we denote current node with dash
                apis.add(storageNodeInUse);
            } else {
                apis.add(new StorageKeyValueApiHttpClient(baseUrl));
            }
        }
        return new CoordinatorKeyValueApi(WCL, RCL, timeout, apis);
    }

    @Order(0)
    @Bean(name = "storageNode")
    KeyValueApi storageNode() {
        String nodeName = Env.get(Env.KVNODE_NAME).orElseGet(Application::randomNodeName);
        Application.nodeName = nodeName;
        storageNodeInUse = new PostgresPersistentKeyValueApi(nodeName);
        return storageNodeInUse;
    }

}
