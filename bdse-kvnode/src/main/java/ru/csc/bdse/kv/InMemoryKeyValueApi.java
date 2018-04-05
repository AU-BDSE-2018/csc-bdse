package ru.csc.bdse.kv;

import ru.csc.bdse.util.Require;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Trivial in-memory implementation of the storage unit.
 *
 * @author semkagtn
 */
public final class InMemoryKeyValueApi implements KeyValueApi {

    private final String name;
    private final ConcurrentMap<String, byte[]> map = new ConcurrentHashMap<>();

    private NodeStatus status;

    public InMemoryKeyValueApi(final String name) {
        Require.nonEmpty(name, "empty name");
        this.name = name;
        status = NodeStatus.UP;
    }

    @Override
    public void put(final String key, final byte[] value) {
        Require.nonEmpty(key, "empty key");
        Require.nonNull(value, "null value");
        map.put(key, value);
    }

    @Override
    public Optional<byte[]> get(final String key) {
        Require.nonEmpty(key, "empty key");
        return Optional.ofNullable(map.get(key));
    }

    @Override
    public Set<String> getKeys(String prefix) {
        Require.nonNull(prefix, "null prefix");
        return map.keySet()
                .stream()
                .filter(key -> key.startsWith(prefix))
                .collect(Collectors.toSet());
    }

    @Override
    public void delete(final String key) {
        Require.nonEmpty(key, "empty key");
        map.remove(key);
    }

    @Override
    public Set<NodeInfo> getInfo() {
        return Collections.singleton(new NodeInfo(name, status));
    }

    @Override
    public void action(String node, NodeAction action) {
        if (!name.equals(node)) {
            return;
        }

        if (action == NodeAction.DOWN) {
            status = NodeStatus.DOWN;
        } else {
            status = NodeStatus.UP;
        }
    }

}
