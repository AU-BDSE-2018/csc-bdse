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
 * Tests for [[ModNPartitioner]]
 *
 * @author alesavin
 */
public class ModNPartitionerTest {

    @Test
    public void mapsByModN() {
        Set<String> partitions = new HashSet<>(Arrays.asList("0", "1", "2"));
        final Partitioner partitioner = new ModNPartitioner(partitions);
        assertThat(partitioner.getPartition("123")).isEqualTo("0");
        assertThat(partitioner.getPartition("1234")).isEqualTo("1");
        assertThat(partitioner.getPartition("12345")).isEqualTo("0");
        assertThat(partitioner.getPartition("Aaaa")).isEqualTo("2");
    }

    @Test
    public void moveHalfOfKeysThenRebalance() {

        Set<String> keys =
                Stream.generate(() -> RandomStringUtils.randomAlphanumeric(10)).limit(1000).collect(Collectors.toSet());

        final Set<String> partitions = new HashSet<>(Arrays.asList("0", "1", "2"));
        final Map<String, String> map = PartitionerUtils.getAll(
                new ModNPartitioner(partitions),
                keys);

        final Set<String> partitions2 = new HashSet<>(Arrays.asList("0", "2"));
        final Map<String, String> map2 = PartitionerUtils.getAll(
                new ModNPartitioner(partitions2),
                keys);

        // There more than half of the keys to be moved
        assertThat(PartitionerUtils.moves(map, map2)).as("moves")
                .isGreaterThan(keys.size() / 2);
    }
}