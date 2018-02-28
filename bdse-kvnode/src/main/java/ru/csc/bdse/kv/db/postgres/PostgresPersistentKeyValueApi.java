package ru.csc.bdse.kv.db.postgres;

import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.jetbrains.annotations.NotNull;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;
import ru.csc.bdse.kv.NodeStatus;
import ru.csc.bdse.kv.db.Entity;
import ru.csc.bdse.kv.db.PersistentKeyValueApi;
import ru.csc.bdse.util.containers.postgres.PostgresContainerManager;

import java.util.Collections;
import java.util.Set;

public final class PostgresPersistentKeyValueApi extends PersistentKeyValueApi {

    @NotNull
    private final NodeInfo state;

    public PostgresPersistentKeyValueApi(@NotNull String name) {
        this.state = new NodeInfo(name, NodeStatus.DOWN);
        this.action(name, NodeAction.UP);
        factory = getFactory();
    }

    @NotNull
    private SessionFactory getFactory() {
        return new Configuration().configure("hibernate_postgres.cfg.xml")
                .addAnnotatedClass(Entity.class)
                .buildSessionFactory();
    }

    @Override
    protected NodeStatus getStatus() {
        return state.getStatus();
    }

    @Override
    public Set<NodeInfo> getInfo() {
        return Collections.singleton(state);
    }

    private boolean changingStatus(NodeAction action) {
        return (state.getStatus() == NodeStatus.UP && action == NodeAction.DOWN) ||
                (state.getStatus() == NodeStatus.DOWN && action == NodeAction.UP);
    }

    @Override
    public void action(String node, NodeAction action) {
        if (!node.equals(state.getName()) || !changingStatus(action)) {
            return;
        }

        System.out.println("Handling action " + action);

        final String containerName = "bdse-postgres-db";
        boolean managerSucceed;

        switch (action) {
            case UP:
                managerSucceed = new PostgresContainerManager().run(containerName);
                if (managerSucceed) {
                    try {
                        factory.close();
                    } catch (HibernateException e) {
                        System.err.println("Error while closing factory: " + e);
                        e.printStackTrace();
                    }
                    factory = getFactory(); // need to rebuild it
                }
                break;
            case DOWN:
                managerSucceed = PostgresContainerManager.stop(containerName);
                break;
            default:
                // unreachable
                throw new RuntimeException("???");
        }

        if (managerSucceed) {
            state.setStatus(action);
        }
    }
}
