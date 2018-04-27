package ru.csc.bdse.kv;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test have to be implemented
 *
 * @author alesavin
 */

// Насколько я понял, у вас важен порядок тестов.
// Т.к. я не знаю нормального способа в JUnit упорядочить их (он не гарантирует order),
// то пришлось их немного переименовать и заюзать фишку из JUnit 4.11
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class AbstractPartitionedKeyValueApiHttpClientTest {

    protected abstract KeyValueApi newCluster1();
    protected abstract KeyValueApi newCluster2();

    protected abstract Set<String> keys();
    protected abstract double expectedKeysLossProportion();
    protected abstract double expectedUndeletedKeysProportion();

    private KeyValueApi cluster1 = newCluster1();
    private KeyValueApi cluster2 = newCluster2();
    private Set<String> keys = keys();

    @Test
    public void test1put1000KeysAndReadItCorrectlyOnCluster1() {
        keys.forEach(k -> cluster1.put(k, k.getBytes()));

        final Set<String> retrievedValues = new HashSet<>();
        for (String key: keys) {
            Optional<byte[]> getRes = cluster1.get(key);
            retrievedValues.add(new String(getRes.orElseThrow(() -> new RuntimeException("test fail"))));
        }

        assertEquals(keys, retrievedValues);
    }

    @Test
    public void test2readKeysFromCluster2AndCheckLossProportion() {
        int nKeys = countOnCluster(cluster2);
        int keysLost = keys.size() - nKeys;
        double keysLossProportion = keysLost * 1.0 / keys.size();
        assertEquals(expectedKeysLossProportion(), keysLossProportion, 1e-9);
    }

    @Test
    public void test3deleteAllKeysFromCluster2() {
        keys.forEach(k -> cluster2.delete(k));
        // TODO try to delete all keys on cluster2
        // Ammm, what should I test here?
        // It's not clear from test name and TODO description
    }

    @Test
    public void test4readKeysFromCluster1AfterDeletionAtCluster2() {
        int keysLeft = countOnCluster(cluster1);
        double keysLeftProportion = keysLeft * 1.0 / keys.size();
        assertEquals(expectedUndeletedKeysProportion(), keysLeftProportion, 1e-9);
    }

    private int countOnCluster(KeyValueApi cluster) {
        int nKeys = 0;
        for (String key: keys) {
            Optional<byte[]> getResult = cluster.get(key);
            if (getResult.isPresent()) {
                nKeys++;
                assertTrue(keys.contains(new String(getResult.get())));
            }
        }
        return nKeys;
    }

}


