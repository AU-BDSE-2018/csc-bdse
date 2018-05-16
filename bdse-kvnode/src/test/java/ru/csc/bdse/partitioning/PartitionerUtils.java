package ru.csc.bdse.partitioning;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Some convenient methods for tests
 *
 * @author alesavin
 */
public class PartitionerUtils {

    public static Map<String, String> getAll(final Partitioner partitioner,
                                             final Set<String> keys) {
        return keys.stream().collect(Collectors.toMap(key -> key, partitioner::getPartition));
    }

    public static Map<String, Integer> statistics(Map<String, String> all) {
        return all.values().stream().collect(Collectors.groupingBy(
                v -> v,
                Collectors.reducing(0, e -> 1, Integer::sum)));
    }

    public static int moves(Map<String, String> left,
                            Map<String, String> right) {
        int moves = 0;
        for (String key: left.keySet()) {
            String lp = left.get(key);
            String rp = right.get(key);
            if (!lp.equals(rp)) moves++;
        }
        return moves;
    }

}
