package ru.csc.bdse.kv;

import org.apache.commons.lang.RandomStringUtils;
import ru.csc.bdse.partitioning.ModNPartitioner;
import ru.csc.bdse.partitioning.PartitionedKeyValueApi;
import ru.csc.bdse.partitioning.Partitioner;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SecondConfigurationPartitionedKeyValueApiHttpClientTest extends AbstractPartitionedKeyValueApiHttpClientTest {

    private static final String node0Name = "node-0";
    private static final String node1Name = "node-1";
    private static final String node2Name = "node-2";
    private static final String node3Name = "node-3";
    private static final String node4Name = "node-4";

    private static final Set<String> names1 = new HashSet<>(Arrays.asList(node0Name, node1Name, node2Name, node3Name, node4Name));
    private static final Set<String> names2 = new HashSet<>(Arrays.asList(node0Name, node1Name, node2Name));

    private static final KeyValueApi node0 = new InMemoryKeyValueApi(node0Name);
    private static final KeyValueApi node1 = new InMemoryKeyValueApi(node1Name);
    private static final KeyValueApi node2 = new InMemoryKeyValueApi(node2Name);
    private static final KeyValueApi node3 = new InMemoryKeyValueApi(node3Name);
    private static final KeyValueApi node4 = new InMemoryKeyValueApi(node4Name);

    private static final PartitionedKeyValueApi cluster1 = new PartitionedKeyValueApi();
    private static final PartitionedKeyValueApi cluster2 = new PartitionedKeyValueApi();

    private static final Set<String> keys = Stream.generate(() -> RandomStringUtils.random(10)).limit(1000).collect(Collectors.toSet());

    private static final Partitioner partitioner1 = new ModNPartitioner(names1);
    private static final Partitioner partitioner2 = new ModNPartitioner(names2);

    static {
        cluster1.configure(partitioner1, 3);
        cluster1.addPartition(node0Name, node0);
        cluster1.addPartition(node1Name, node1);
        cluster1.addPartition(node2Name, node2);
        cluster1.addPartition(node3Name, node3);
        cluster1.addPartition(node4Name, node4);

        cluster2.configure(partitioner2, 3);
        cluster2.addPartition(node0Name, node0);
        cluster2.addPartition(node1Name, node1);
        cluster2.addPartition(node2Name, node2);
    }

    @Override
    protected KeyValueApi newCluster1() {
        return cluster1;
    }

    @Override
    protected KeyValueApi newCluster2() {
        return cluster2;
    }

    @Override
    protected Set<String> keys() {
        return keys;
    }

    @Override
    protected double expectedKeysLossProportion() {
        /*
            rate = 0.8 потому что те ключи, для которых верно, что
            key.hash % 5 == key.hash % 3, те останутся на месте и будут доступны.
            тогда:
            5z + x == 3k + x, z \in \mathcal{Z}, k \in \mathcal{Z}
            z = 3k / 5. => 5 | k.

            Теперь просто смотрим на числа до 15ти и видим, что только 3 числа подходят под верхнее условие.
            Т.о. мы не теряем 1/5, а значит теряем 1 - 1/5 = 0.8
         */

        double rate = 0.8;
        double sigma = Math.sqrt(rate * (1 - rate) / keys.size());
        return rate + 3 * sigma;
    }

    @Override
    protected double expectedUndeletedKeysProportion() {
        return expectedKeysLossProportion();
    }

}
