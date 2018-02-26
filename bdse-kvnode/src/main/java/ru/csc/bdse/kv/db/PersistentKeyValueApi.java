package ru.csc.bdse.kv.db;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import ru.csc.bdse.kv.KeyValueApi;

import java.util.*;
import java.util.function.Function;

/**
 * Implementation of persistent key-value storage.
 *
 */
public abstract class PersistentKeyValueApi implements KeyValueApi {

    protected SessionFactory factory;

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
    @SuppressWarnings("unchecked")
    public Set<String> getKeys(String prefix) {
        return runQuery(session -> {
            Set<String> result = new HashSet<>();
            List<Entity> entities = session.createQuery("FROM Entity WHERE _key LIKE " + prefix + "%").list();
            entities.forEach(entity -> result.add(entity.getKey()));
            return result;
        });
    }

    @Override
    public void delete(String key) {
        runQuery(session -> {
            Entity entity = session.load(Entity.class, key);
            session.delete(entity);
            return null;
        });
    }

}
