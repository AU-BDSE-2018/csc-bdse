package ru.csc.bdse.partitioning;

import java.util.Set;

/**
 * Selects partition by consistent hashing circle
 *
 * @author alesavin
 */
public class ConsistentHashMd5Partitioner implements Partitioner {

    private final ConsistentHash ch;

    public ConsistentHashMd5Partitioner(Set<String> partitions) {
        this.ch = new ConsistentHash(HashingFunctions.md5Function, partitions);
    }

    @Override
    public String getPartition(String key) {
        return ch.get(key);
    }
}
