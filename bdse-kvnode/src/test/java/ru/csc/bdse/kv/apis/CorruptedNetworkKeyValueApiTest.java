package ru.csc.bdse.kv.apis;

import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

public final class CorruptedNetworkKeyValueApiTest implements KeyValueApi {

    @Override
    public void put(String key, byte[] value) {
        // do nothing, so that we respond with 200 HTTP status code
    }

    @Override
    public Optional<byte[]> get(String key) {
        return Optional.of("some bytes that don't form valid RecordWithTimestamp".getBytes());
    }

    @Override
    public Set<String> getKeys(String prefix) {
        return Collections.emptySet();
    }

    @Override
    public void delete(String key) {
        // do nothing, so that we respond with 200 HTTP status code
    }

    @Override
    public Set<NodeInfo> getInfo() {
        return Collections.emptySet();
    }

    @Override
    public void action(String node, NodeAction action) {
        throw new UnsupportedOperationException();
    }

}
