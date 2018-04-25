package ru.csc.bdse.kv;

import org.junit.Test;

/**
 * Test have to be implemented
 *
 * @author alesavin
 */
public abstract class AbstractPartitionedKeyValueApiHttpClientTest {

/*
    protected abstract KeyValueApi newCluster1();
    protected abstract KeyValueApi newCluster2();
    // Stream.generate(() -> RandomStringUtils.randomAlphanumeric(10)).limit(1000).collect(Collectors.toSet());
    protected abstract Set<String> keys();
    protected abstract float expectedKeysLossProportion();
    protected abstract float expectedUndeletedKeysProportion();

    private KeyValueApi cluster1 = newCluster1();
    private KeyValueApi cluster2 = newCluster2();

    private Set<String> keys = keys();
*/

    @Test
    public void put1000KeysAndReadItCorrectlyOnCluster1() {
        // TODO put 1000 keys to storage ant read it
    }

    @Test
    public void readKeysFromCluster2AndCheckLossProportion() {
        // TODO read all keys from cluster2 and made some statistics (related to expectedKeysLossProportion)
    }

    @Test
    public void deleteAllKeysFromCluster2() {
        // TODO try to delete all keys on cluster2
    }

    @Test
    public void readKeysFromCluster1AfterDeletionAtCluster2() {
        // TODO read all keys from cluster1, made some statistics (related to expectedUndeletedKeysProportion)
    }
}


