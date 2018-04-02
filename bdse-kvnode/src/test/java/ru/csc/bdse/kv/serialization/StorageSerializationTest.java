package ru.csc.bdse.kv.serialization;


import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Test;
import ru.csc.bdse.kv.serialzation.StorageSerializationUtils;
import ru.csc.bdse.serialization.Proto;

import static org.junit.Assert.assertArrayEquals;

public final class StorageSerializationTest {

    @Test
    public void simpleSerialization() throws InvalidProtocolBufferException {
        final String value = "some value\nwith\t\rall ._1 kinds of  symbols\0\0\1\2\3\4";

        final byte[] serializeKeyValue = StorageSerializationUtils.serializeKeyValue(value.getBytes());
        final Proto.RecordWithTimestamp record = StorageSerializationUtils.deserializeRecord(serializeKeyValue);

        assertArrayEquals(value.getBytes(), record.getValue().toByteArray());
    }

}
