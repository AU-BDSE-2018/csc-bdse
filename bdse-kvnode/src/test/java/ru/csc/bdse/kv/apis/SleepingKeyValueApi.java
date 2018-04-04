package ru.csc.bdse.kv.apis;

import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;

import java.util.Optional;
import java.util.Set;

public final class SleepingKeyValueApi implements KeyValueApi {

    @Override
    public void put(String key, byte[] value) {
        sleep();
    }

    @Override
    public Optional<byte[]> get(String key) {
        sleep();
        return null;
    }

    @Override
    public Set<String> getKeys(String prefix) {
        sleep();
        return null;
    }

    @Override
    public void delete(String key) {
        sleep();
    }

    @Override
    public Set<NodeInfo> getInfo() {
        sleep();
        return null;
    }

    @Override
    public void action(String node, NodeAction action) {
        sleep();
    }

    private void sleep() {
        while (true) {
            try {
                Thread.sleep(10000000);
            } catch (Exception e) {
                // ignore
            }
        }
    }

}
