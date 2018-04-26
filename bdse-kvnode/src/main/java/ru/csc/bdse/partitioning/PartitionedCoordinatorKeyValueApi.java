package ru.csc.bdse.partitioning;

import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PartitionedCoordinatorKeyValueApi implements KeyValueApi {

    private final ExecutorService executor = Executors.newCachedThreadPool();

    private Partitioner partitioner;
    private int timeout = 1;
    private Map<String, KeyValueApi> partitions = new HashMap<>();

    public void configure(Partitioner partitioner, int timeout) {
        this.partitioner = partitioner;
        this.timeout = timeout;
    }

    public void addPartition(String name, KeyValueApi replica) {
        partitions.put(name, replica);
    }

    @Override
    public void put(String key, byte[] value) {
        final KeyValueApi partition = getPartition(key);
        final Callable<Void> putTask = () -> {partition.put(key, value); return null;};
        executeTask(putTask, "put for key " + key + " failed");
    }

    @Override
    public Optional<byte[]> get(String key) {
        final KeyValueApi partition = getPartition(key);
        final Callable<Optional<byte[]>> getTask = () -> partition.get(key);
        return executeTask(getTask, "get for key " + key + " failed");
    }

    @Override
    public Set<String> getKeys(String prefix) {
        return partitions.values()
                .stream()
                .map(partition -> partition.getKeys(prefix))
                .reduce((s1, s2) -> {s1.addAll(s2); return s1;})
                .orElseThrow(() -> new RuntimeException("getKeys failed"));
    }

    @Override
    public void delete(String key) {
        final KeyValueApi partition = getPartition(key);
        final Callable<Void> deleteTask = () -> {partition.delete(key); return null;};
        executeTask(deleteTask, "delete for key " + key + " failed");
    }

    @Override
    public Set<NodeInfo> getInfo() {
        return partitions.values()
                .stream()
                .map(KeyValueApi::getInfo)
                .reduce((s1, s2) -> {s1.addAll(s2); return s1;})
                .orElseThrow(() -> new RuntimeException("getInfo failed"));
    }

    @Override
    public void action(String node, NodeAction action) {
        final KeyValueApi partition = partitions.get(node);
        final Callable<Void> actionTask = () -> {partition.action(node, action); return null;};
        executeTask(actionTask, "action for node" + node + " failed");
    }

    private KeyValueApi getPartition(String key) {
        final String partitionName = partitioner.getPartition(key);
        return partitions.get(partitionName);
    }

    private <T> T executeTask(Callable<T> task, String errLog) {
        try {
            return executor.submit(task).get(timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            System.err.println(errLog);
            throw new RuntimeException(e);
        }
    }

}
