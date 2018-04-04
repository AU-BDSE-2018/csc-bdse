package ru.csc.bdse.kv.replication;

import ru.csc.bdse.serialization.Proto;

import java.util.Map;
import java.util.Set;

public interface ConflictResolver {

    /**
     * resolves conflicts in replications' responses on {@link ru.csc.bdse.kv.KeyValueApi#get} query
     * @param responses map of (key, value) where @key is node's ID and @value is its response
     * @return the record which we suppose is the real one
     */
    Proto.RecordWithTimestamp resolve(Map<Integer, Proto.RecordWithTimestamp> responses);

    /**
     * resolves conflicts in replications' responses on {@link ru.csc.bdse.kv.KeyValueApi#getKeys} query
     * by joining them all
     * @param responses map of (key, value) where @key is node's ID and @value is its response
     * @return resolved set of keys
     */
    Set<String> resolveKeys(Map<Integer, Set<String>> responses);

}
