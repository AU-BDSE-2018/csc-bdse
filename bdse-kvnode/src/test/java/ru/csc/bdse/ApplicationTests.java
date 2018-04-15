package ru.csc.bdse;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@Ignore
public class ApplicationTests {

    @Test
	public void contextLoads() {
//        final KeyValueApi api = new ControllerKeyValueApiHttpClient("http://localhost:" + 8080);
//        api.put("somekey", "some value".getBytes());
//        System.out.println(new String(api.get("somekey").orElse(":(".getBytes())));
	}
}
