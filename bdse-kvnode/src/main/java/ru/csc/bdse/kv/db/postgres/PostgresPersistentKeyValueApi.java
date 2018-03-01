package ru.csc.bdse.kv.db.postgres;

import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;
import ru.csc.bdse.kv.NodeStatus;
import ru.csc.bdse.kv.db.Entity;
import ru.csc.bdse.kv.db.PersistentKeyValueApi;
import ru.csc.bdse.util.containers.ContainerManager;
import ru.csc.bdse.util.containers.postgres.PostgresContainerManager;

import java.util.Collections;
import java.util.Set;

public final class PostgresPersistentKeyValueApi extends PersistentKeyValueApi {

    @NotNull
    private final NodeInfo state;

    public PostgresPersistentKeyValueApi(@NotNull String name) {
        this.state = new NodeInfo(name, NodeStatus.DOWN);
        this.action(name, NodeAction.UP);
    }

    @NotNull
    private SessionFactory getFactory(@Nullable String containerIp) {
        if (containerIp == null) {
            throw new IllegalArgumentException("Container's IP can't be null");
        }
        final String connectionUrl =  String.format("jdbc:postgresql://%s:5432/postgres", containerIp);
        return new Configuration().configure("hibernate_postgres.cfg.xml")
                .addAnnotatedClass(Entity.class)
                .setProperty("hibernate.connection.url", connectionUrl)
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

        final String containerName = "bdse-postgres-db-" + state.getName();
        boolean managerSucceed;

        switch (action) {
            case UP:
                managerSucceed = new PostgresContainerManager().run(containerName);
                if (managerSucceed) {
                    try {
                        if (factory != null) {
                            factory.close();
                        }
                    } catch (HibernateException e) {
                        System.err.println("Error while closing factory: " + e);
                        e.printStackTrace();
                    }
                    factory = getFactory(ContainerManager.getContainerIp(containerName)); // need to rebuild it
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
