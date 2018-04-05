package ru.csc.bdse.kv;

import ru.csc.bdse.util.Require;

import java.util.Objects;

/**
 * Represents node information
 *
 * @author alesavin
 */
public final class NodeInfo {

    private String name;
    private NodeStatus status;

    public NodeInfo(String name, NodeStatus status) {
        Require.nonNull(name, "null name");
        Require.nonNull(status, "null status");

        this.name = name;
        this.status = status;
    }

    private NodeInfo() {
    }

    public String getName() {
        return name;
    }

    public NodeStatus getStatus() {
        return status;
    }

    public void setStatus(NodeAction action) {
        switch (action) {
            case UP:
                status = NodeStatus.UP;
                break;
            case DOWN:
                status = NodeStatus.DOWN;
                break;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeInfo nodeInfo = (NodeInfo) o;
        return Objects.equals(name, nodeInfo.name) && status == nodeInfo.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "NodeInfo{" +
                "name='" + name + '\'' +
                ", status=" + status +
                '}';
    }
}
