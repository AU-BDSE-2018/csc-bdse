package ru.csc.bdse.kv;

import ru.csc.bdse.util.Require;

import java.util.*;
import java.util.concurrent.*;

public final class ReplicatedKeyValueApi implements KeyValueApi {

    private final String name;
    private final ExecutorService executor;

    private int WCL = 1;
    private int RCL = 1;
    private int timeout = 0;
    private List<KeyValueApi> replics = Collections.emptyList();

    public ReplicatedKeyValueApi(final String name) {
        Require.nonEmpty(name, "empty name");
        this.name = name;
        executor = Executors.newCachedThreadPool();
    }

    public void setWCL(int WCL) {
        this.WCL = WCL;
    }

    public void setRCL(int RCL) {
        this.RCL = RCL;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public void setReplics(List<KeyValueApi> replics) {
        this.replics = replics;
    }

    public void configure(int WCL, int RCL, int timeout, List<KeyValueApi> replics) {
        setWCL(WCL);
        setRCL(RCL);
        setTimeout(timeout);
        setReplics(replics);
    }

    @Override
    public void put(String key, byte[] value) {
        List<Future<Integer>> replicaResults = new ArrayList<>();
        for (KeyValueApi replica: replics) {
            Callable<Integer> putTask = () -> {
                try {
                    replica.put(key, value);
                    return 1;
                } catch (Exception e) {
                    return 0;
                }
            };
            replicaResults.add(executor.submit(putTask));
        }

        int successfulWrites = 0;

        for (Future<Integer> replicaResult: replicaResults) {
            try {
                successfulWrites += replicaResult.get(timeout, TimeUnit.SECONDS);
            } catch (TimeoutException|ExecutionException|InterruptedException e) {
                // ignore, just leave successfulWrites the same
            }
        }

        // should we throw an exception here? `put` is void
        if (successfulWrites < WCL) {
            throw new RuntimeException("only got " + successfulWrites + " responses. WCL is set to " + WCL);
        }
    }

    @Override
    public Optional<byte[]> get(String key) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public Set<String> getKeys(String prefix) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void delete(String key) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public Set<NodeInfo> getInfo() {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void action(String node, NodeAction action) {
        throw new UnsupportedOperationException("");
    }

}
