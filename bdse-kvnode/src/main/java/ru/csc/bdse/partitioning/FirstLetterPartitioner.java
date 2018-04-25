package ru.csc.bdse.partitioning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Selects partition by first letter of a key
 *
 * @author alesavin
 */
public class FirstLetterPartitioner implements Partitioner {

    private final List<String> partitions;

    public FirstLetterPartitioner(Set<String> partitions) {
        List<String> list = new ArrayList<>(partitions);
        Collections.sort(list);
        this.partitions = list;
    }

    @Override
    public String getPartition(String key) {
        int index = key.charAt(0) * partitions.size() / ((int)Character.MAX_VALUE + 1);
        return partitions.get(index);
    }
}
