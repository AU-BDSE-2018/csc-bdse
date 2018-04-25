package ru.csc.bdse.partitioning;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.Function;

/**
 * Provide some hashing for strings
 *
 * @author alesavin
 */
public final class HashingFunctions {

    public final static Function<String, Integer> hashCodeFunction =
            String::hashCode;

    public final static Function<String, Integer> md5Function = (String s) -> {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(s.getBytes());
            byte[] bytes = md.digest();
            return (bytes[0] & 0xFF)
                    | ((bytes[1] & 0xFF) << 8)
                    | ((bytes[2] & 0xFF) << 16)
                    | ((bytes[3] & 0xFF) << 24);
        } catch (NoSuchAlgorithmException e) {
            return 0;
        }
    };

    private static final int DEFAULT_SEED = 104729;
    // Constants for 32 bit variant
    private static final int C1_32 = 0xcc9e2d51;
    private static final int C2_32 = 0x1b873593;
    private static final int R1_32 = 15;
    private static final int R2_32 = 13;
    private static final int M_32 = 5;
    private static final int N_32 = 0xe6546b64;

    /**
     * Murmur3 is successor to Murmur2 fast non-crytographic hash algorithms.
     *
     * 32-bit Java port of https://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp#94
     */
    public final static Function<String, Integer> murmur3Function = (String s) -> {
        byte[] data = s.getBytes();
        int length = data.length;

        int hash = DEFAULT_SEED;
        final int nblocks = length >> 2;

        // body
        for (int i = 0; i < nblocks; i++) {
            int i_4 = i << 2;
            int k = (data[i_4] & 0xff)
                    | ((data[i_4 + 1] & 0xff) << 8)
                    | ((data[i_4 + 2] & 0xff) << 16)
                    | ((data[i_4 + 3] & 0xff) << 24);

            // mix functions
            k *= C1_32;
            k = Integer.rotateLeft(k, R1_32);
            k *= C2_32;
            hash ^= k;
            hash = Integer.rotateLeft(hash, R2_32) * M_32 + N_32;
        }

        // tail
        int idx = nblocks << 2;
        int k1 = 0;
        switch (length - idx) {
            case 3:
                k1 ^= data[idx + 2] << 16;
            case 2:
                k1 ^= data[idx + 1] << 8;
            case 1:
                k1 ^= data[idx];

                // mix functions
                k1 *= C1_32;
                k1 = Integer.rotateLeft(k1, R1_32);
                k1 *= C2_32;
                hash ^= k1;
        }

        // finalization
        hash ^= length;
        hash ^= (hash >>> 16);
        hash *= 0x85ebca6b;
        hash ^= (hash >>> 13);
        hash *= 0xc2b2ae35;
        hash ^= (hash >>> 16);

        return hash;
    };
}