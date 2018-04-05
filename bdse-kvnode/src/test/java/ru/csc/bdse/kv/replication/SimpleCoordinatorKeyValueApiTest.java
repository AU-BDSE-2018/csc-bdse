package ru.csc.bdse.kv.replication;

import org.junit.Before;
import org.junit.Test;
import ru.csc.bdse.kv.InMemoryKeyValueApi;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static ru.csc.bdse.kv.NodeStatus.DOWN;
import static ru.csc.bdse.kv.NodeStatus.UP;

public final class SimpleCoordinatorKeyValueApiTest {

    private static final List<KeyValueApi> inMemoryApis = new ArrayList<>();
    private static KeyValueApi api;

    @Before
    public void init() {
        inMemoryApis.clear();
        inMemoryApis.add(new InMemoryKeyValueApi("1"));
        inMemoryApis.add(new InMemoryKeyValueApi("2"));
        inMemoryApis.add(new InMemoryKeyValueApi("3"));


        CoordinatorKeyValueApi coordinatorKeyValueApi = new CoordinatorKeyValueApi();
        coordinatorKeyValueApi.configure(3, 3, 1);
        inMemoryApis.forEach(coordinatorKeyValueApi::addReplica);
        api = coordinatorKeyValueApi;
    }

    @Test
    public void simplePutGetTest() {
        final String key = "some key";
        final String value = "some value";
        api.put(key, value.getBytes());
        final Optional<byte[]> res = api.get(key);

        assertTrue(res.isPresent());
        assertEquals(value, new String(res.get()));
    }

    @Test
    public void deleteTest() {
        final String key = "some key";
        api.put(key, "some value".getBytes());

        Optional<byte[]> res = api.get(key);
        assertTrue(res.isPresent());

        api.delete(key);
        res = api.get(key);
        assertTrue(!res.isPresent());
    }

    @Test
    public void getKeysTest() {
        final String prefix = "key";
        final String key1 = prefix + "1";
        final String key2 = prefix + "2";
        final String key3 = prefix + "3";
        final String key4 = prefix + "4";
        final String key5 = "keY5";

        assertTrue(api.getKeys("").isEmpty());

        inMemoryApis.get(0).put(key1, "".getBytes());
        inMemoryApis.get(1).put(key2, "".getBytes());
        inMemoryApis.get(2).put(key3, "".getBytes());
        inMemoryApis.get(2).put(key1, "".getBytes());
        api.put(key4, "".getBytes());
        api.put(key5, "".getBytes());

        final Set<String> answer = new HashSet<>(Arrays.asList(key1, key2, key3, key4));

        assertEquals(answer, api.getKeys(prefix));
    }

    @Test
    public void getInfoTest() {
        final Set<NodeInfo> answer = new HashSet<>();
        for (KeyValueApi inMemoryApi: inMemoryApis) {
            answer.addAll(inMemoryApi.getInfo());
        }

        assertEquals(answer, api.getInfo());
    }

    @Test
    public void actionTest() {
        final Set<NodeInfo> answer = new HashSet<>();
        answer.add(new NodeInfo("1", UP));
        answer.add(new NodeInfo("2", DOWN));
        answer.add(new NodeInfo("3", DOWN));

        api.action("2", NodeAction.DOWN);
        api.action("3", NodeAction.DOWN);

        api.action("1", NodeAction.DOWN);
        api.action("1", NodeAction.UP);

        final Set<NodeInfo> res = api.getInfo();
        assertEquals(answer, res);
    }

}