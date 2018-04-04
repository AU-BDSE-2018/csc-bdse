package ru.csc.bdse.kv.replication;

import org.junit.BeforeClass;
import org.junit.Test;
import ru.csc.bdse.kv.InMemoryKeyValueApi;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.apis.FailingKeyValueApi;
import ru.csc.bdse.kv.apis.SleepingKeyValueApi;
import ru.csc.bdse.kv.db.postgres.PostgresPersistentKeyValueApi;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.*;

public final class AdvancedCoordinatorKeyValueApiTest {

    private static final List<KeyValueApi> apis = new ArrayList<>();

    @BeforeClass
    public static void init() {
        apis.add(new FailingKeyValueApi());
        apis.add(new SleepingKeyValueApi());
        apis.add(new InMemoryKeyValueApi("in-memory-1"));
        apis.add(new InMemoryKeyValueApi("in-memory-2"));
        apis.add(new PostgresPersistentKeyValueApi("postgres-1"));
    }

    @Test
    public void testCrud() {
        final KeyValueApi api = new CoordinatorKeyValueApi(3, 3, 1, apis);

        final String key1 = "key1";
        final String key2 = "key2";
        final String key3 = "\"key3\"";

        final String value1 = "value1\0\1\n\t\r";
        final String value2 = "\0\1\2\3value\1\2\3";
        final String value11 = "   another_one\\\"\' ";

        api.put(key1, value1.getBytes());
        api.put(key2, value2.getBytes());

        assertFalse(api.get(key3).isPresent());

        Optional<byte[]> getResult = api.get(key1);

        assertTrue(getResult.isPresent());
        assertEquals(value1, new String(getResult.get()));

        api.delete(key1);
        assertFalse(api.get(key1).isPresent());

        api.put(key1, value11.getBytes());
        getResult = api.get(key1);
        assertTrue(getResult.isPresent());
        assertEquals(value11, new String(getResult.get()));

        api.put(key1, value1.getBytes());
        getResult = api.get(key1);
        assertTrue(getResult.isPresent());
        assertEquals(value1, new String(getResult.get()));

        api.delete(key3); // tests that deleting non-existing value is ok

        assertEquals(2, api.getKeys("key").size());
        api.delete(key1);
        final Set<String> keys = api.getKeys("key");
        assertEquals(1, keys.size());
        api.put(key3, value11.getBytes());
        assertEquals(1, api.getKeys("\"").size());
        assertEquals(0, api.getKeys("K").size());
    }

    @Test(expected = RuntimeException.class)
    public void testPutFail() {
        final KeyValueApi api = new CoordinatorKeyValueApi(4, 3, 1, apis);
        api.put("some key", "some value".getBytes());
    }

    @Test(expected = RuntimeException.class)
    public void testGetFail() {
        final KeyValueApi api = new CoordinatorKeyValueApi(3, 4, 1, apis);
        api.put("some key", "some value".getBytes());
        api.get("some key");
    }

}
