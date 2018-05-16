package ru.csc.bdse.kv;

import org.apache.commons.lang.RandomStringUtils;
import ru.csc.bdse.partitioning.FirstLetterPartitioner;
import ru.csc.bdse.partitioning.PartitionedKeyValueApi;
import ru.csc.bdse.partitioning.Partitioner;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class FirstConfigurationPartitionedKeyValueApiHttpClientTest extends AbstractPartitionedKeyValueApiHttpClientTest {

    private static final String node0Name = "node-0";
    private static final String node1Name = "node-1";
    private static final String node2Name = "node-2";

    private static final Set<String> names1 = new HashSet<>(Arrays.asList(node0Name, node1Name, node2Name));
    private static final Set<String> names2 = new HashSet<>(Arrays.asList(node0Name, node2Name));

    private static final KeyValueApi node0 = new InMemoryKeyValueApi(node0Name);
    private static final KeyValueApi node1 = new InMemoryKeyValueApi(node1Name);
    private static final KeyValueApi node2 = new InMemoryKeyValueApi(node2Name);

    private static final PartitionedKeyValueApi cluster1 = new PartitionedKeyValueApi();
    private static final PartitionedKeyValueApi cluster2 = new PartitionedKeyValueApi();

    private static final Set<String> keys = Stream.generate(() -> RandomStringUtils.random(10)).limit(1000).collect(Collectors.toSet());

    private static final Partitioner partitioner1 = new FirstLetterPartitioner(names1);
    private static final Partitioner partitioner2 = new FirstLetterPartitioner(names2);

    static {
        cluster1.configure(partitioner1, 3);
        cluster1.addPartition(node0Name, node0);
        cluster1.addPartition(node1Name, node1);
        cluster1.addPartition(node2Name, node2);

        cluster2.configure(partitioner2, 3);
        cluster2.addPartition(node0Name, node0);
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
            rate = 1/3 потому что те ключи, что были на node-0 и node-2 так же туда и будут попадать,
            а ключи из node-1 будут попалам между ними разделены и мы их не увидим.
         */

        double rate = 1f / 3f;
        double sigma = Math.sqrt(rate * (1 - rate) / keys.size());
        return rate + 3 * sigma;
    }

    @Override
    protected double expectedUndeletedKeysProportion() {
        return expectedKeysLossProportion();
    }

}
