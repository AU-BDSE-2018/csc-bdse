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
 * Tests for [[FirstLetterPartitioner]]
 *
 * @author alesavin
 */
public class FirstLetterPartitionerTest {

    @Test
    public void mapsByFirstLetter() {
        Set<String> partitions = new HashSet<>(Arrays.asList("0", "1", "2"));
        final Partitioner partitioner = new FirstLetterPartitioner(partitions);
        assertThat(partitioner.getPartition("\u0000")).isEqualTo("0");
        assertThat(partitioner.getPartition("\uFFFF")).isEqualTo("2");
        assertThat(partitioner.getPartition("\uFFFA")).isEqualTo("2");

        assertThat(partitioner.getPartition("Aaaaa")).isEqualTo("0");
        assertThat(partitioner.getPartition("A")).isEqualTo("0");
        assertThat(partitioner.getPartition("affff")).isEqualTo("0");
        assertThat(partitioner.getPartition("a123")).isEqualTo("0");
        assertThat(partitioner.getPartition("b")).isEqualTo("0");
        assertThat(partitioner.getPartition("c")).isEqualTo("0");
        assertThat(partitioner.getPartition("D")).isEqualTo("0");
        assertThat(partitioner.getPartition("z")).isEqualTo("0");
        assertThat(partitioner.getPartition("Zde")).isEqualTo("0");

        char c1 = (char)(Character.MAX_VALUE / partitions.size());
        assertThat(partitioner.getPartition(new String(new char[]{c1}))).isEqualTo("0");
        assertThat(partitioner.getPartition(new String(new char[]{(char)(c1 + 1)}))).isEqualTo("1");
        assertThat(partitioner.getPartition(new String(new char[]{(char)(c1 + 5)}))).isEqualTo("1");
        assertThat(partitioner.getPartition(new String(new char[]{(char)(c1 * 2 + 1)}))).isEqualTo("2");
    }

    @Test
    public void mapsByFirstLetterInRanges() {
        Set<String> partitions = new HashSet<>(Arrays.asList("0", "1", "2"));
        final Partitioner partitioner = new FirstLetterPartitioner(partitions);
        char c1 = (char)(Character.MAX_VALUE / partitions.size());
        assertThat(partitioner.getPartition(new String(new char[]{c1}))).isEqualTo("0");
        assertThat(partitioner.getPartition(new String(new char[]{(char)(c1 + 1)}))).isEqualTo("1");
        assertThat(partitioner.getPartition(new String(new char[]{(char)(c1 + 5)}))).isEqualTo("1");
        assertThat(partitioner.getPartition(new String(new char[]{(char)(c1 * 2)}))).isEqualTo("1");
        assertThat(partitioner.getPartition(new String(new char[]{(char)(c1 * 3)}))).isEqualTo("2");
        assertThat(partitioner.getPartition(new String(new char[]{(char)(c1 * 4)}))).isEqualTo("0");
        assertThat(partitioner.getPartition(new String(new char[]{(char)(c1 * 5)}))).isEqualTo("1");
        assertThat(partitioner.getPartition(new String(new char[]{(char)(c1 * 6)}))).isEqualTo("2");
        assertThat(partitioner.getPartition(new String(new char[]{(char)(c1 * 7)}))).isEqualTo("0");
    }

    @Test
    public void moveAllOfKeysThenRebalance() {

        Set<String> keys =
                Stream.generate(() -> RandomStringUtils.randomNumeric(10)).limit(1000).collect(Collectors.toSet());

        final Set<String> partitions = new HashSet<>(Arrays.asList("0", "1", "2"));
        final Map<String, String> map = PartitionerUtils.getAll(
                new FirstLetterPartitioner(partitions),
                keys);

        final Set<String> partitions2 = new HashSet<>(Arrays.asList("1", "2"));
        final Map<String, String> map2 = PartitionerUtils.getAll(
                new FirstLetterPartitioner(partitions2),
                keys);

        // There move all of the keys
        assertThat(PartitionerUtils.moves(map, map2)).as("moves")
                .isGreaterThanOrEqualTo(keys.size());
    }
}