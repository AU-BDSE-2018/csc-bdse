package ru.csc.bdse.kv.apis;

import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;

import java.util.Optional;
import java.util.Set;

public final class FailingKeyValueApi implements KeyValueApi {

    @Override
    public void put(String key, byte[] value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<byte[]> get(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> getKeys(String prefix) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<NodeInfo> getInfo() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void action(String node, NodeAction action) {
        throw new UnsupportedOperationException();
    }

}
