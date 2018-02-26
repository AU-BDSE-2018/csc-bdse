package ru.csc.bdse.kv.db;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.jetbrains.annotations.NotNull;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;
import ru.csc.bdse.kv.NodeStatus;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Implementation of persistent key-value storage.
 *
 */
public final class PersistentKeyValueApi implements KeyValueApi {

    @NotNull private final SessionFactory factory;
    @NotNull private final NodeInfo state;

    public PersistentKeyValueApi(@NotNull String name) {
        this.state = new NodeInfo(name, NodeStatus.UP);

        try {
            factory = new Configuration().configure()
                    .addAnnotatedClass(Entity.class)
                    .buildSessionFactory();
        } catch (Throwable ex) {
            System.err.println("Failed to create sessionFactory object." + ex);
            throw new ExceptionInInitializerError(ex);
        }
    }

    private <T> T runQuery(Function<Session, T> fun) {
        Transaction tx = null;
        T res = null;

        try (final Session session = factory.openSession()) {
            tx = session.beginTransaction();
            res = fun.apply(session);
            tx.commit();
        } catch (HibernateException e) {
            if (tx != null) {
                tx.rollback();
            }
            e.printStackTrace();
        }

        return res;
    }

    @Override
    public void put(String key, byte[] value) {
        runQuery(session -> {
            Entity entity = new Entity(key, value);
            session.saveOrUpdate(entity);
            return null;
        });
    }

    @Override
    public Optional<byte[]> get(String key) {
        return Optional.ofNullable(runQuery(session -> {
            Entity entity = session.get(Entity.class, key);
            if (entity != null) {
                return entity.getValue();
            } else {
                return null;
            }
        }));
    }

    @Override
    public Set<String> getKeys(String prefix) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(String key) {
        runQuery(session -> {
            Entity entity = session.load(Entity.class, key);
            session.delete(entity);
            return null;
        });
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
