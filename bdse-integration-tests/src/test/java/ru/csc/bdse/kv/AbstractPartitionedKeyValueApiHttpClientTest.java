package ru.csc.bdse.kv;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.HashSet;
import java.util.Optional;
import java.util.Random;
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
    // Stream.generate(() -> RandomStringUtils.randomAlphanumeric(10)).limit(1000).collect(Collectors.toSet());
    protected abstract Set<String> keys();
    protected abstract float expectedKeysLossProportion();
    protected abstract float expectedUndeletedKeysProportion();

    private KeyValueApi cluster1 = newCluster1();
    private KeyValueApi cluster2 = newCluster2();

    private Set<String> keys = keys();

    @Test
    public void test1put1000KeysAndReadItCorrectlyOnCluster1() {
        final Random rand = new Random();
        keys.forEach(k -> {
            if (rand.nextInt() % 2 == 0) {
                cluster1.put(k, k.getBytes());
            } else {
                cluster2.put(k, k.getBytes());
            }
        });

        final Set<String> retrievedValues = new HashSet<>();
        keys.forEach(k -> retrievedValues.add(new String(cluster1.get(k).orElseThrow(() -> new RuntimeException("test fail")))));

        assertEquals(keys, retrievedValues);
    }

    @Test
    public void test2readKeysFromCluster2AndCheckLossProportion() {
        int nKeys = 0;
        for (String key: keys) {
            Optional<byte[]> getResult = cluster2.get(key);
            if (getResult.isPresent()) {
                nKeys++;
                assertTrue(keys.contains(new String(getResult.get())));
            } else {
                nKeys--;
            }
        }

        int keysLost = keys.size() - nKeys;
        float keysLossProportion = (float)keysLost / keys.size();
        assertTrue(Math.abs(expectedKeysLossProportion() - keysLossProportion) < 1.0 / keys.size());
    }

    @Test
    public void test3deleteAllKeysFromCluster2() {
        // TODO try to delete all keys on cluster2
        keys.forEach(k -> cluster2.delete(k));
        // Ammm, what should I test here?
        // It's not clear from test name and TODO description
    }

    @Test
    public void test4readKeysFromCluster1AfterDeletionAtCluster2() {
        // TODO read all keys from cluster1, made some statistics (related to expectedUndeletedKeysProportion)
        int keysLeft = 0;
        for (String key: keys) {
            Optional<byte[]> getResult = cluster1.get(key);
            if (getResult.isPresent()) {
                keysLeft++;
                assertTrue(keys.contains(new String(getResult.get())));
            } else {
                keysLeft--;
            }
        }

        float keysLeftProportion = (float)keysLeft / keys.size();
        assertTrue(Math.abs(expectedUndeletedKeysProportion() - keysLeftProportion) < 1.0 / keys.size());
    }
}


