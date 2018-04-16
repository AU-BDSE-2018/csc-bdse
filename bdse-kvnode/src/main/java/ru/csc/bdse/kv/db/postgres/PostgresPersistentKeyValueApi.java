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
import ru.csc.bdse.util.Env;
import ru.csc.bdse.util.containers.ContainerManager;
import ru.csc.bdse.util.containers.postgres.PostgresContainerManager;

import java.util.Collections;
import java.util.Set;

public final class PostgresPersistentKeyValueApi extends PersistentKeyValueApi {

    @NotNull
    private final NodeInfo state;
    private String hbm2ddl = "create";

    public PostgresPersistentKeyValueApi(@NotNull String name) {
        this.state = new NodeInfo(name, NodeStatus.DOWN);
        this.action(name, NodeAction.UP);
    }

    @NotNull
    private SessionFactory getFactory(@Nullable String containerHost) {
        if (containerHost == null) {
            throw new IllegalArgumentException("Container's IP can't be null");
        }
        final String connectionUrl =  String.format("jdbc:postgresql://%s:5432/postgres", containerHost);
        return new Configuration().configure("hibernate_postgres.cfg.xml")
                .addAnnotatedClass(Entity.class)
                .setProperty("hibernate.connection.url", connectionUrl)
                .setProperty("hibernate.hbm2ddl.auto", hbm2ddl)
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

        System.out.println(node + ": Handling action " + action);

        final String containerName = "bdse-postgres-db-" + state.getName();
        final String networkName = Env.get(Env.NETWORK_NAME).orElse("bridge");
        boolean managerSucceed;

        switch (action) {
            case UP:
                managerSucceed = new PostgresContainerManager().run(containerName, networkName);
                if (managerSucceed) {
                    try {
                        if (factory != null) {
                            factory.close();
                        }
                    } catch (HibernateException e) {
                        System.err.println("Error while closing factory: " + e);
                        e.printStackTrace();
                    }
                    factory = getFactory(ContainerManager.getContainerHost(containerName, networkName)); // need to rebuild it
                    // TODO for some unknown to me reason "validate" option does not work.
                    // Seems like it creates table in lowercase and validates in normal (entity vs Entity)
                    // anyways, just disable for now.
                    hbm2ddl = "none";
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
