package ru.csc.bdse.partitioning;

/**
 * Maps key to partition
 *
 * @author alesavin
 */
public interface Partitioner {

    /**
     * Selects right partition for the key
     */
    String getPartition(String key);
}
