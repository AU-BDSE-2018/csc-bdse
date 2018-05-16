package ru.csc.bdse.partitioning;


import java.util.Collection;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * Represents consistent hashing circle
 *  See https://web.archive.org/web/20120605030524/http://weblogs.java.net/blog/tomwhite/archive/2007/11/consistent_hash.html
 *
 * @author alesavin
 */
public class ConsistentHash {

    private final Function<String, Integer> hashFunction;
    private final int factor;
    private final SortedMap<Integer, String> circle = new TreeMap<>();

    public ConsistentHash(final Function<String, Integer> hashFunction,
                          final Set<String> nodes) {
        this(hashFunction, nodes, 1);
    }

    public ConsistentHash(final Function<String, Integer> hashFunction,
                          final Set<String> nodes,
                          int factor) {
        this.hashFunction = hashFunction;
        this.factor = factor;

        for (String node : nodes) {
            add(node);
        }
    }

    private void add(String node) {
        for (int i = 0; i < factor; i++) {
            circle.put(hashFunction.apply(node + i), node);
        }
    }

    public void remove(String node) {
        for (int i = 0; i < factor; i++) {
            circle.remove(hashFunction.apply(node + i));
        }
    }

    public String get(String key) {
        if (circle.isEmpty()) {
            throw new IllegalStateException("ConsistentHash circle is empty");
        }
        int hash = hashFunction.apply(key);
        if (!circle.containsKey(hash)) {
            SortedMap<Integer, String> tailMap = circle.tailMap(hash);
            hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        return circle.get(hash);
    }

}