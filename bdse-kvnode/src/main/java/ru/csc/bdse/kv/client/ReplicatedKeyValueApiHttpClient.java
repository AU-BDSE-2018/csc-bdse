package ru.csc.bdse.kv.client;

import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public final class ReplicatedKeyValueApiHttpClient implements KeyValueApi {

    private final List<KeyValueApi> apis = new ArrayList<>();

    public ReplicatedKeyValueApiHttpClient(final List<String> baseUrls) {
        for (String baseUrl: baseUrls) {
            apis.add(new ControllerKeyValueApiHttpClient(baseUrl));
        }
    }

    @Override
    public void put(String key, byte[] value) {
        for (KeyValueApi api: apis) {
            try {
                api.put(key, value);
                return;
            } catch (Exception e) {
                // ignore
            }
        }

        throw new RuntimeException("Failed to put");
    }

    @Override
    public Optional<byte[]> get(String key) {
        for (KeyValueApi api: apis) {
            try {
                return api.get(key);
            } catch (Exception e) {
                // just ignore
            }
        }

        throw new RuntimeException("Failed to get");
    }

    @Override
    public Set<String> getKeys(String prefix) {
        for (KeyValueApi api: apis) {
            try {
                return api.getKeys(prefix);
            } catch (Exception e) {
                // just ignore
            }
        }

        throw new RuntimeException("Failed to getKeys");
    }

    @Override
    public void delete(String key) {
        for (KeyValueApi api: apis) {
            try {
                api.delete(key);
                return;
            } catch (Exception e) {
                // ignore
            }
        }

        throw new RuntimeException("Failed to delete");
    }

    @Override
    public Set<NodeInfo> getInfo() {
        for (KeyValueApi api: apis) {
            try {
                return api.getInfo();
            } catch (Exception e) {
                // ignore
            }
        }

        throw new RuntimeException("Failed to getInfo");
    }

    @Override
    public void action(String node, NodeAction action) {
        for (KeyValueApi api: apis) {
            try {
                api.action(node, action);
                return;
            } catch (Exception e) {
                // ignore
            }
        }

        throw new RuntimeException("Failed to action");
    }

}
