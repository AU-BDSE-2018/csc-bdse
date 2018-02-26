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

import java.util.Optional;
import java.util.Set;

/**
 * Implementation of persistent key-value storage.
 *
 */
public final class PersistentKeyValueApi implements KeyValueApi {

    @NotNull private final SessionFactory factory;
    @NotNull private final String name;

    public PersistentKeyValueApi(@NotNull String name) {
        this.name = name;

        try {
            factory = new Configuration().configure()
                    .addAnnotatedClass(Entity.class)
                    .buildSessionFactory();
        } catch (Throwable ex) {
            System.err.println("Failed to create sessionFactory object." + ex);
            throw new ExceptionInInitializerError(ex);
        }
    }

    @Override
    public void put(String key, byte[] value) {
        Transaction tx = null;

        try (final Session session = factory.openSession()) {
            Entity entity = new Entity(key, value);
            tx = session.beginTransaction();
            session.saveOrUpdate(entity);
            tx.commit();
        } catch (HibernateException e) {
            if (tx != null) {
                tx.rollback();
            }
            e.printStackTrace();
        }
    }

    @Override
    public Optional<byte[]> get(String key) {
        Transaction tx = null;
        Entity entity = null;

        try (final Session session = factory.openSession()) {
            tx = session.beginTransaction();
            entity = session.get(Entity.class, key);
            tx.commit();
        } catch (HibernateException e) {
            if (tx != null) {
                tx.rollback();
            }
            e.printStackTrace();
        }

        if (entity != null) {
            return Optional.of(entity.getValue());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Set<String> getKeys(String prefix) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<NodeInfo> getInfo() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void action(String node, NodeAction action) {
        throw new UnsupportedOperationException();
    }

}
