package ru.csc.bdse.util;

import ru.csc.bdse.partitioning.ConsistentHashMd5Partitioner;
import ru.csc.bdse.partitioning.FirstLetterPartitioner;
import ru.csc.bdse.partitioning.ModNPartitioner;

import java.util.Arrays;
import java.util.List;

/**
 * @author semkagtn
 */
public final class Constants {

    private Constants() {

    }

    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    public static List<String> PARTITIONERS = Arrays.asList(
            FirstLetterPartitioner.class.getName(),
            ModNPartitioner.class.getName(),
            ConsistentHashMd5Partitioner.class.getName()
    );
}
