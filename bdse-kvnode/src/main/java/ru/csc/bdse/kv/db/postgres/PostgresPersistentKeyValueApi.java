package ru.csc.bdse.kv.db.postgres;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.jetbrains.annotations.NotNull;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;
import ru.csc.bdse.kv.NodeStatus;
import ru.csc.bdse.kv.db.Entity;
import ru.csc.bdse.kv.db.PersistentKeyValueApi;

import java.util.Collections;
import java.util.Set;

public final class PostgresPersistentKeyValueApi extends PersistentKeyValueApi {

    @NotNull
    private final NodeInfo state;

    public PostgresPersistentKeyValueApi(@NotNull String name) {
        this.state = new NodeInfo(name, NodeStatus.UP);
    }

    @Override
    @NotNull
    protected SessionFactory getFactory() {
        return new Configuration().configure("hibernate_postgres.cfg.xml")
                .addAnnotatedClass(Entity.class)
                .buildSessionFactory();
    }

    @Override
    public Set<NodeInfo> getInfo() {
        return Collections.singleton(state);
    }

    @Override
    public void action(String node, NodeAction action) {
        if (node.equals(state.getName())) {
            state.setStatus(action);
        }
    }
}
