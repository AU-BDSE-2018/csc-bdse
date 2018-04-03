package ru.csc.bdse.kv.replication;

import org.junit.BeforeClass;
import org.junit.Test;
import ru.csc.bdse.kv.InMemoryKeyValueApi;
import ru.csc.bdse.kv.KeyValueApi;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReplicatedKeyValueApiTest {

    private static final List<KeyValueApi> inMemoryApis = new ArrayList<>();
    private static KeyValueApi api;

    @BeforeClass
    public static void init() {
        inMemoryApis.add(new InMemoryKeyValueApi("1"));
        inMemoryApis.add(new InMemoryKeyValueApi("2"));
        inMemoryApis.add(new InMemoryKeyValueApi("3"));
        api = new ReplicatedKeyValueApi("replicated", 3, 3, 1, inMemoryApis);
    }

    @Test
    public void simplePutGetTest() {
        final String value = "some value";
        api.put("some key", value.getBytes());
        final Optional<byte[]> res = api.get("some key");

        assertTrue(res.isPresent());
        assertEquals(value, new String(res.get()));
    }

}