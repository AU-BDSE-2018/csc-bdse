package ru.csc.bdse.kv.replication;

import com.google.protobuf.ByteString;
import org.junit.BeforeClass;
import org.junit.Test;
import ru.csc.bdse.kv.serialzation.StorageSerializationUtils;
import ru.csc.bdse.serialization.Proto;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class ConflictResolverImplTest {

    private static final ConflictResolver resolver = new ConflictResolverImpl();

    private static Proto.RecordWithTimestamp firstRecord;
    private static Proto.RecordWithTimestamp secondRecord;

    // this two records have the same timestamp but different values
    private static Proto.RecordWithTimestamp thirdRecord;
    private static Proto.RecordWithTimestamp fourthRecord;

    @BeforeClass
    public static void init() throws Exception {
        final byte[] firstEncodedRecord = StorageSerializationUtils.serializeKeyValue("1".getBytes());
        firstRecord = StorageSerializationUtils.deserializeRecord(firstEncodedRecord);

        // Don't handle it so that we know if it's interrupted exception
        // so that records can not have the same timestamp
        Thread.sleep(10);

        final byte[] secondEncodedRecord = StorageSerializationUtils.serializeKeyValue("2".getBytes());
        secondRecord = StorageSerializationUtils.deserializeRecord(secondEncodedRecord);

        final byte[] thirdEncodedRecord = StorageSerializationUtils.serializeKeyValue("3".getBytes());
        thirdRecord = StorageSerializationUtils.deserializeRecord(thirdEncodedRecord);

        final Proto.RecordWithTimestamp.Builder fourthRecordBuilder = Proto.RecordWithTimestamp.newBuilder();
        fourthRecordBuilder.setValue(ByteString.copyFrom("4".getBytes()));
        fourthRecordBuilder.setTimestamp(thirdRecord.getTimestamp());
        fourthRecordBuilder.setIsDeleted(false);

        fourthRecord = fourthRecordBuilder.build();
    }

    @Test
    public void resolveFirstRule() {
        Map<Integer, Proto.RecordWithTimestamp> recordsMap = new HashMap<>();
        recordsMap.put(1, firstRecord);
        recordsMap.put(2, secondRecord);

        final Proto.RecordWithTimestamp resolvedRecord = resolver.resolve(recordsMap);

        assertSame(secondRecord, resolvedRecord);
    }

    @Test
    public void resolveSecondRule() {
        Map<Integer, Proto.RecordWithTimestamp> recordsMap = new HashMap<>();
        recordsMap.put(1, thirdRecord);
        recordsMap.put(2, thirdRecord);
        recordsMap.put(3, fourthRecord);

        final Proto.RecordWithTimestamp resolvedRecord = resolver.resolve(recordsMap);

        assertSame(thirdRecord, resolvedRecord);
    }

    @Test
    public void resolveThirdRule() {
        Map<Integer, Proto.RecordWithTimestamp> recordsMap = new HashMap<>();
        recordsMap.put(1, fourthRecord);
        recordsMap.put(2, fourthRecord);
        recordsMap.put(3, thirdRecord);
        recordsMap.put(4, thirdRecord);

        final Proto.RecordWithTimestamp resolvedRecord = resolver.resolve(recordsMap);

        assertSame(fourthRecord, resolvedRecord);
    }

    @Test
    public void resolveKeys() {
        final String value1 = "Value-1";
        final String value2 = "Value-2";
        final String value3 = "Value-3";
        final String value4 = "Value-4";

        Map<Integer, Set<String>> nodeToKeys = new HashMap<>();
        nodeToKeys.put(1, new HashSet<>(Arrays.asList(value1, value2, value4)));
        nodeToKeys.put(2, Collections.singleton(value2));
        nodeToKeys.put(3, Collections.singleton(value3));

        final Set<String> answer = new HashSet<>(Arrays.asList(value1, value2, value3, value4));
        final Set<String> resolveResult = resolver.resolveKeys(nodeToKeys);

        assertEquals(answer, resolveResult);
    }

}
