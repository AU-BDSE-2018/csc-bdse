package ru.csc.bdse.partitioning;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Java6Assertions.assertThat;

/**
 * Tests for [[ConsistentHash]]
 *
 * @author alesavin
 */
public class ConsistentHashTest {

    private static final boolean DEBUG_OUTPUT = false;
    private int keysCount = 100;

    @Test
    public void workForOneNodeAndHashCode() {
        Set<String> nodes = Collections.singleton("0");
        final ConsistentHash ch =
                new ConsistentHash(HashingFunctions.hashCodeFunction, nodes);
        assertThat(ch.get("00")).as("00").isEqualTo("0");
        assertThat(ch.get("0")).as("0").isEqualTo("0");
        assertThat(ch.get("k0")).as("k0").isEqualTo("0");
        assertThat(ch.get("asdjasjkdjka")).as("asdjasjkdjka").isEqualTo("0");
        assertThat(ch.get("-123")).as("-123").isEqualTo("0");
        final ConsistentHash ch2 =
                new ConsistentHash(HashingFunctions.hashCodeFunction, nodes);
        assertThat(ch2.get("0")).as("0").isEqualTo("0");
        assertThat(ch2.get("k0")).as("k0").isEqualTo("0");
        assertThat(ch2.get("asdjasjkdjka")).as("asdjasjkdjka").isEqualTo("0");
        assertThat(ch2.get("-123")).as("-123").isEqualTo("0");
    }

    @Test
    public void nonEvenlyPlaceKeysForThreeNodesAndHashCode() {
        Set<String> nodes = new HashSet<>(Arrays.asList("0", "1", "2"));
        final ConsistentHash ch =
                new ConsistentHash(HashingFunctions.hashCodeFunction, nodes);
        Set<String> keys =
                Stream.generate(() -> RandomStringUtils.random(10)).limit(keysCount).collect(Collectors.toSet());
        Map<String, Integer> statistics = getStatistics(nodes, keys, ch);
        assertThat(statistics.get("0")).as("0 counts").isEqualTo(keysCount);
    }

    @Test
    public void evenlyPlaceKeysForThreeNodesAndMd5() {
        Set<String> nodes = new HashSet<>(Arrays.asList("0", "1", "2"));
        final ConsistentHash ch =
                new ConsistentHash(HashingFunctions.md5Function, nodes);
        Set<String> keys =
                Stream.generate(() -> RandomStringUtils.random(10)).limit(keysCount).collect(Collectors.toSet());
        Map<String, Integer> statistics = getStatistics(nodes, keys, ch);
        for (String node: nodes) {
            assertThat(statistics.get(node)).as(node + " counts").isLessThan(keysCount * 2 / nodes.size() );
        }
    }

    @Test
    public void rebalanceKeysAfterRemoveForFiveNodesAndMd5() {
        Set<String> nodes = new HashSet<>(Arrays.asList("0", "1", "2", "3", "4"));
        final ConsistentHash ch =
                new ConsistentHash(HashingFunctions.md5Function, nodes);
        Set<String> keys =
                Stream.generate(() -> RandomStringUtils.random(10)).limit(keysCount).collect(Collectors.toSet());
        String nodeToRemove =
                new ArrayList<>(nodes).get(new Random().nextInt(nodes.size()));
        Map<String, Integer> statisticsBeforeRemove = getStatistics(nodes, keys, ch);
        ch.remove(nodeToRemove);
        Map<String, Integer> statisticsAfterRemove = getStatistics(nodes, keys, ch);
        assertThat(statisticsAfterRemove.get(nodeToRemove)).as(nodeToRemove + " counts").isEqualTo(0);

        int movedKeys = 0;
        for (String node: nodes) {
            if (!nodeToRemove.equals(node)) {
                int diff = statisticsAfterRemove.get(node) - statisticsBeforeRemove.get(node);
                assertThat(diff).as(node + " diff").isGreaterThanOrEqualTo(0);
                movedKeys += diff;
            }
        }
        assertThat(movedKeys).as("moved keys").isEqualTo(statisticsBeforeRemove.get(nodeToRemove));
    }

    @Test
    public void rebalanceKeysForTwoConfigsForFiveNodesAndMd5() {
        Set<String> nodes = new HashSet<>(Arrays.asList("0", "1", "2", "3", "4"));
        final ConsistentHash ch =
                new ConsistentHash( HashingFunctions.md5Function, nodes);
        Set<String> keys =
                Stream.generate(() -> RandomStringUtils.random(10)).limit(keysCount).collect(Collectors.toSet());

        Map<String, Integer> statisticsBefore = getStatistics(nodes, keys, ch);

        String nodeToRemove =
                new ArrayList<>(nodes).get(new Random().nextInt(nodes.size()));
        Set<String> nodes2 =
                nodes.stream().filter(n -> !nodeToRemove.equals(n)).collect(Collectors.toSet());
        final ConsistentHash ch2 =
                new ConsistentHash( HashingFunctions.md5Function, nodes2);

        Map<String, Integer> statisticsAfter = getStatistics(nodes2, keys, ch2);

        int movedKeys = 0;
        for (String node: nodes) {
            if (!nodeToRemove.equals(node)) {
                int diff = statisticsAfter.get(node) - statisticsBefore.get(node);
                assertThat(diff).as(node + " diff").isGreaterThanOrEqualTo(0);
                movedKeys += diff;
            }
        }
        assertThat(movedKeys).as("moved keys").isEqualTo(statisticsBefore.get(nodeToRemove));
    }

    private Map<String, Integer> getStatistics(Collection<String> nodes,
                                               Collection<String> keys,
                                               ConsistentHash ch) {

        Map<String, Integer> statistics = new HashMap<>();
        for (String node: nodes) statistics.put(node, 0);

        for (String key: keys) {
            String node = ch.get(key);
            statistics.put(node, statistics.get(node) + 1);
        }

        if (DEBUG_OUTPUT) {
            for (String node : nodes) {
                System.out.println(node + " = " + statistics.get(node));
            }
        }
        return statistics;
    }
}