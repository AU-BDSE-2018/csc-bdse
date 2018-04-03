package ru.csc.bdse.kv.serialzation;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import ru.csc.bdse.serialization.Proto;

import java.time.Instant;

public final class StorageSerializationUtils {

    public static byte[] serializeRecord(byte[] value, boolean isDeleted) {
        final Instant time = Instant.now();
        final Timestamp timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond())
                .setNanos(time.getNano()).build();

        final Proto.RecordWithTimestamp.Builder builder = Proto.RecordWithTimestamp.newBuilder();
        builder.setValue(ByteString.copyFrom(value))
                .setIsDeleted(isDeleted)
                .setTimestamp(timestamp);

        final Proto.RecordWithTimestamp record = builder.build();
        return record.toByteArray();
    }

    public static byte[] serializeRecord(byte[] value) {
        return serializeRecord(value, false);
    }

    public static Proto.RecordWithTimestamp deserializeRecord(byte[] data) throws InvalidProtocolBufferException {
        return Proto.RecordWithTimestamp.parseFrom(data);
    }

}
