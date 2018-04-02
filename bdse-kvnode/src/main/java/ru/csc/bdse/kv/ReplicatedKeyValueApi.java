package ru.csc.bdse.kv;

import ru.csc.bdse.util.Require;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public final class ReplicatedKeyValueApi implements KeyValueApi {

    private final String name;

    private int WCL = 1;
    private int RCL = 1;
    private double timeout = 0;
    private List<KeyValueApi> replics = Collections.emptyList();

    public ReplicatedKeyValueApi(final String name) {
        Require.nonEmpty(name, "empty name");
        this.name = name;
    }

    public void setWCL(int WCL) {
        this.WCL = WCL;
    }

    public void setRCL(int RCL) {
        this.RCL = RCL;
    }

    public void setTimeout(double timeout) {
        this.timeout = timeout;
    }

    public void setReplics(List<KeyValueApi> replics) {
        this.replics = replics;
    }

    public void configure(int WCL, int RCL, double timeout, List<KeyValueApi> replics) {
        setWCL(WCL);
        setRCL(RCL);
        setTimeout(timeout);
        setReplics(replics);
    }

    @Override
    public void put(String key, byte[] value) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public Optional<byte[]> get(String key) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public Set<String> getKeys(String prefix) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void delete(String key) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public Set<NodeInfo> getInfo() {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void action(String node, NodeAction action) {
        throw new UnsupportedOperationException("");
    }

}
