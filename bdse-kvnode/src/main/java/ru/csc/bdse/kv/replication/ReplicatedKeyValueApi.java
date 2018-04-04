package ru.csc.bdse.kv.replication;

import com.google.protobuf.Timestamp;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;
import ru.csc.bdse.kv.serialzation.StorageSerializationUtils;
import ru.csc.bdse.serialization.Proto;
import ru.csc.bdse.util.Require;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public final class ReplicatedKeyValueApi implements KeyValueApi {

    private final String name;
    private final ExecutorService executor;
    private final ConflictResolver conflictResolver = new ConflictResolverImpl();

    private int WCL;
    private int RCL;
    private int timeout;
    private List<KeyValueApi> replics;

    public ReplicatedKeyValueApi(final String name, int WCL, int RCL, int timeout, List<KeyValueApi> replics) {
        Require.nonEmpty(name, "empty name");
        executor = Executors.newCachedThreadPool();
        this.name = name;
        this.WCL = WCL;
        this.RCL = RCL;
        this.timeout = timeout;
        this.replics = replics;
    }

    @Override
    public void put(String key, byte[] value) {
        final byte[] rawRecord = StorageSerializationUtils.serializeRecord(value);
        putRawRecord(key, rawRecord);
    }

    @Override
    public Optional<byte[]> get(String key) {
        final List<Future<Integer>> replicaResults = new ArrayList<>();
        final Map<Integer, Optional<byte[]>> returnedRawRecords = new HashMap<>();

        for (int i = 0; i < replics.size(); i++) {
            final int id = i;
            Callable<Integer> getTask = () -> {
                try {
                    returnedRawRecords.put(id, replics.get(id).get(key));
                    return 1;
                } catch (Exception e) {
                    return 0;
                }
            };
            replicaResults.add(executor.submit(getTask));
        }

        int successfulReads = 0;

        for (Future<Integer> replicaResult: replicaResults) {
            try {
                successfulReads += replicaResult.get(timeout, TimeUnit.SECONDS);
            } catch (TimeoutException|ExecutionException|InterruptedException e) {
                // ignore, just leave successfulReads the same
            }
        }

        // should we throw an exception here? `put` is void
        if (successfulReads < RCL) {
            throw new RuntimeException("only got " + successfulReads + " responses. RCL is set to " + RCL);
        }

        // can't return null from collect, so need a record marking a fault
        final Proto.RecordWithTimestamp.Builder faultRecordBuilder = Proto.RecordWithTimestamp.newBuilder();
        faultRecordBuilder.setTimestamp(Timestamp.newBuilder().setSeconds(-1).setNanos(-1).build());
        final Proto.RecordWithTimestamp faultRecord = faultRecordBuilder.build();

        // filter out empty ones
        final Map<Integer, Proto.RecordWithTimestamp> returnedRecords = returnedRawRecords.entrySet()
                .stream()
                .filter(e -> e.getValue().isPresent())
                .collect(Collectors.toMap(Map.Entry::getKey,
                        e -> {
                            final byte[] rawRecord = e.getValue().orElseThrow(() -> new IllegalStateException("impossible"));
                            try {
                                return StorageSerializationUtils.deserializeRecord(rawRecord);
                            } catch (Exception ex) {
                                return faultRecord;
                            }
                        }))
                .entrySet()
                .stream()
                .filter(e -> !e.getValue().equals(faultRecord))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (returnedRecords.size() == 0) {
            return Optional.empty();
        }

        final Proto.RecordWithTimestamp record = conflictResolver.resolve(returnedRecords);

        if (record.getIsDeleted()) {
            return Optional.empty();
        }

        return Optional.of(record.getValue().toByteArray());
    }

    // TODO does not filter-out deleted keys. Should I run "get" on each of this keys? brr, ugly.
    @Override
    public Set<String> getKeys(String prefix) {
        // TODO copy-paste from get()
        final List<Future<Integer>> replicaResults = new ArrayList<>();
        final Map<Integer, Set<String>> returnedKeys = new HashMap<>();

        for (int i = 0; i < replics.size(); i++) {
            final int id = i;
            Callable<Integer> getTask = () -> {
                try {
                    returnedKeys.put(id, replics.get(id).getKeys(prefix));
                    return 1;
                } catch (Exception e) {
                    return 0;
                }
            };
            replicaResults.add(executor.submit(getTask));
        }

        int successfulReads = 0;

        for (Future<Integer> replicaResult: replicaResults) {
            try {
                successfulReads += replicaResult.get(timeout, TimeUnit.SECONDS);
            } catch (TimeoutException|ExecutionException|InterruptedException e) {
                // ignore, just leave successfulReads the same
            }
        }

        // should we throw an exception here? `put` is void
        if (successfulReads < RCL) {
            throw new RuntimeException("only got " + successfulReads + " responses. RCL is set to " + RCL);
        }

        return conflictResolver.resolveKeys(returnedKeys);
    }

    @Override
    public void delete(String key) {
        final byte[] deletedRecord =
                StorageSerializationUtils.serializeRecord("".getBytes(), true);
        putRawRecord(key, deletedRecord);
    }

    @Override
    public Set<NodeInfo> getInfo() {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void action(String node, NodeAction action) {
        throw new UnsupportedOperationException("");
    }

    private void putRawRecord(String key, byte[] rawRecord) {
        final List<Future<Integer>> replicaResults = new ArrayList<>();
        for (KeyValueApi replica: replics) {
            Callable<Integer> putTask = () -> {
                try {
                    replica.put(key, rawRecord);
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

}
