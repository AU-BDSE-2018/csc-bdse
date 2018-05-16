package ru.csc.bdse.partitioning;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Java6Assertions.assertThat;

/**
 * Tests for [[ConsistentHashMd5Partitioner]]
 *
 * @author alesavin
 */
public class ConsistentHashMd5PartitionerTest {

    @Test
    public void moveLessThanHalfOfKeysThenRebalance() {

        Set<String> keys =
                Stream.generate(() -> RandomStringUtils.randomAlphanumeric(10)).limit(1000).collect(Collectors.toSet());

        final Set<String> partitions = new HashSet<>(Arrays.asList("0", "1", "2"));
        final Map<String, String> map = PartitionerUtils.getAll(
                new ConsistentHashMd5Partitioner(partitions),
                keys);

        final Set<String> partitions2 = new HashSet<>(Arrays.asList("0", "2"));
        final Map<String, String> map2 = PartitionerUtils.getAll(
                new ConsistentHashMd5Partitioner(partitions2),
                keys);

        // There less than half of the keys to be moved
        assertThat(PartitionerUtils.moves(map, map2)).as("moves")
                .isLessThan(keys.size() / 2);
    }
}