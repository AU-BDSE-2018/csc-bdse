package ru.csc.bdse.kv.replication;

import com.google.protobuf.Timestamp;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;
import ru.csc.bdse.kv.serialzation.StorageSerializationUtils;
import ru.csc.bdse.serialization.Proto;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public final class CoordinatorKeyValueApi implements KeyValueApi {

    private final ExecutorService executor;
    private final ConflictResolver conflictResolver = new ConflictResolverImpl();

    private int WCL;
    private int RCL;
    private int timeout;
    private List<KeyValueApi> replics;

    public CoordinatorKeyValueApi(int WCL, int RCL, int timeout, List<KeyValueApi> replics) {
        executor = Executors.newCachedThreadPool();
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


        /*
         * We need to filter out deleted records from these keys. I don't see how to do it
         * without explicitly calling `get` for each key. This approach is super inefficient,
         * but seems like we don't have any choice right now.
         */

        final Set<String> resolvedKeys = conflictResolver.resolveKeys(returnedKeys);
        final Set<String> res = new HashSet<>();

        for (String key: resolvedKeys) {
            if (get(key).isPresent()) {
                res.add(key);
            }
        }

        return res;
    }

    @Override
    public void delete(String key) {
        final byte[] deletedRecord =
                StorageSerializationUtils.serializeRecord("".getBytes(), true);
        putRawRecord(key, deletedRecord);
    }

    @Override
    public Set<NodeInfo> getInfo() {
        final Set<NodeInfo> res = new HashSet<>();
        for (KeyValueApi replica: replics) {
            try {
                res.addAll(replica.getInfo());
            } catch (Exception e) {
                // ignore for now
            }
        }

        return res;
    }

    @Override
    public void action(String node, NodeAction action) {
        for (KeyValueApi replica: replics) {
            try {
                replica.action(node, action);
            } catch (Exception e) {
                // TODO I'm not sure what we should do in this case. Ignore for now.
            }
        }
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
