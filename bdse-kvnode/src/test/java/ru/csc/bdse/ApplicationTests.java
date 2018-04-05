package ru.csc.bdse;

import org.junit.Test;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.client.ControllerKeyValueApiHttpClient;

import static org.junit.Assert.assertFalse;

//@RunWith(SpringRunner.class)
//@SpringBootTest
//@Ignore
public class ApplicationTests {

	private static final String nodeName = "node-0";

	@Test
	public void contextLoads() {
		ControllerKeyValueApiHttpClient api = new ControllerKeyValueApiHttpClient("http://localhost:8080");
		api.configure(1, 1, 1);
		api.addReplica("-");

		final String key = "SomeKey";
		final String value = "SomeValue";

		api.put(key, value.getBytes());
		api.action(nodeName, NodeAction.DOWN);
		assertFalse(api.get(key).isPresent());
		api.action(nodeName, NodeAction.UP);

		api.delete(key);
	}
}
