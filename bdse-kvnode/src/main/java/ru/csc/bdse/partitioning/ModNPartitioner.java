package ru.csc.bdse.partitioning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Selects partition by mod N
 *
 * @author alesavin
 */
public class ModNPartitioner implements Partitioner {

    private final List<String> partitions;

    public ModNPartitioner(Set<String> partitions) {
        List<String> list = new ArrayList<>(partitions);
        Collections.sort(list);
        this.partitions = list;
    }

    @Override
    public String getPartition(String key) {
        int index = Math.abs(key.hashCode() % partitions.size());
        return partitions.get(index);
    }
}
