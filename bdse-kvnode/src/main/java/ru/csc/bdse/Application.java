package ru.csc.bdse;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.db.postgres.PostgresPersistentKeyValueApi;
import ru.csc.bdse.util.Env;

import java.util.UUID;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);

        final KeyValueApi node = new Application().node();
//        node.put("SomeKey", "SomeValue".getBytes());
//        node.put("OneMoreKey", "OneMoreValue".getBytes());

        System.out.println(new String(node.get("SomeKey").get()));
        System.out.println(new String(node.get("OneMoreKey").get()));
    }

    private static String randomNodeName() {
        return "kvnode-" + UUID.randomUUID().toString().substring(4);
    }

    @Bean
    KeyValueApi node() {
        String nodeName = Env.get(Env.KVNODE_NAME).orElseGet(Application::randomNodeName);
        return new PostgresPersistentKeyValueApi(nodeName);
    }
}
