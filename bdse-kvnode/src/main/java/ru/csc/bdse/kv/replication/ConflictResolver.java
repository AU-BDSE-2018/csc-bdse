package ru.csc.bdse.kv.replication;

import ru.csc.bdse.serialization.Proto;

import java.util.Map;
import java.util.Set;

public interface ConflictResolver {

    /**
     * resolves conflicts in replications' responses on {@link ru.csc.bdse.kv.KeyValueApi#get} query
     * @param responses map of (key, value) where @key is node's ID (= name) and @value is its response
     * @return the record which is the real value according to the following rules:
     *      1. Compare records' timestamps. If only one record has maximum timestamp, return it.
     *      2. If more than one records have maximum timestamp, take the set of their values
     *      {@link Proto.RecordWithTimestamp#getValue()} and find the most common one. Return
     *      any record with this value.
     *      3. If multiple different values are the most common ones (i.e. the occur the same number of times)
     *      pick the one which was sent by the node with minimum ID (Note: node ID is a string so it will be
     *      a lexicographical order).
     */
    Proto.RecordWithTimestamp resolve(Map<String, Proto.RecordWithTimestamp> responses);

    Set<String> resolveKeys(Map<String, Set<String>> responses);

}
