package ru.csc.bdse;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.client.StorageKeyValueApiHttpClient;
import ru.csc.bdse.kv.db.postgres.PostgresPersistentKeyValueApi;
import ru.csc.bdse.kv.replication.CoordinatorKeyValueApi;
import ru.csc.bdse.partitioning.PartitionedKeyValueApi;
import ru.csc.bdse.partitioning.Partitioner;
import ru.csc.bdse.util.Env;

import javax.annotation.PreDestroy;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import static ru.csc.bdse.util.Constants.PARTITIONERS;

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

    @Bean(name = "partitionNode")
    KeyValueApi partitionNode() throws ClassNotFoundException, InstantiationException,
            IllegalAccessException, InvocationTargetException {
        // TODO get rid of this copy-paste
        System.out.println("partition init. nodeName is " + nodeName);

        final PartitionedKeyValueApi partition = new PartitionedKeyValueApi();

        final String partitionerName = Env.get(Env.PARTITIONER).orElse(PARTITIONERS.get(0));
        final int TIMEOUT = Env.getInt(Env.TIMEOUT).orElse(1);
        final List<String> REPLICS = Env.getList(Env.REPLICS).orElse(Collections.singletonList(nodeName));

        final Partitioner partitioner = (Partitioner)Class
                .forName(partitionerName).getDeclaredConstructors()[0].newInstance(new HashSet<>(REPLICS));
        partition.configure(partitioner, TIMEOUT);
        REPLICS.forEach(name -> {
                    KeyValueApi api;
                    if (name.equals(nodeName)) {
                        api = storageNodeInUse;
                    } else {
                        api = new StorageKeyValueApiHttpClient("http://" + name + ":8080");
                    }
                    partition.addPartition(name, api);
                });

        return partition;
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
        System.out.println("storageNode init. nodeName is " + nodeName);
        storageNodeInUse = new PostgresPersistentKeyValueApi(nodeName);
        System.out.println("storageNode DONE. nodeName is " + nodeName);
        return storageNodeInUse;
    }

}
