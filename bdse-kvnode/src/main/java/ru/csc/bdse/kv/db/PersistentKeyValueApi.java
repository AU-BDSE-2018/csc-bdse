package ru.csc.bdse.kv.db;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;
import ru.csc.bdse.kv.KeyValueApi;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Implementation of persistent key-value storage.
 *
 */
public abstract class PersistentKeyValueApi implements KeyValueApi {

    private final SessionFactory factory = getFactory();

    protected abstract SessionFactory getFactory();

    private <T> T runQuery(Function<Session, T> queryFun) {
        Transaction tx = null;
        T res = null;

        try (final Session session = factory.openSession()) {
            tx = session.beginTransaction();
            res = queryFun.apply(session);
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
            final Entity entity = new Entity(key, value);
            session.saveOrUpdate(entity);
            return null;
        });
    }

    @Override
    public Optional<byte[]> get(String key) {
        return Optional.ofNullable(runQuery(session -> {
            final Entity entity = session.get(Entity.class, key);
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
            final Set<String> result = new HashSet<>();
            final Query q = session.createQuery("FROM Entity WHERE key LIKE ?");
            q.setParameter(0, prefix + "%");
            final List<Entity> entities = q.list();
            entities.forEach(entity -> result.add(entity.getKey()));
            return result;
        });
    }

    @Override
    public void delete(String key) {
        runQuery(session -> {
            final Entity entity = session.load(Entity.class, key);
            session.delete(entity);
            return null;
        });
    }

}
