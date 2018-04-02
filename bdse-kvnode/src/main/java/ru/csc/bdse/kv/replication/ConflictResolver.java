package ru.csc.bdse.kv.replication;

import ru.csc.bdse.serialization.Proto;

import java.util.Set;

public interface ConflictResolver {

    Proto.RecordWithTimestamp resolve(Set<Proto.RecordWithTimestamp> in);

    Set<String> resolveKeys(Set<Set<String>> in);

}
