package ru.csc.bdse.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.DependsOn;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import ru.csc.bdse.Application;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.client.StorageKeyValueApiHttpClient;
import ru.csc.bdse.kv.replication.CoordinatorKeyValueApi;

// We must create coordinator controller only after we've created local storage controller
// FIXME I don't know why, but @Order and @Primary don't affect their creation ordering. So I just do this hack
@DependsOn("storageNode")
@RestController
@RequestMapping("/coordinator")
public final class CoordinatorKeyValueApiController extends KeyValueApiController {

    public CoordinatorKeyValueApiController(@Autowired @Qualifier("coordinatorNode") final KeyValueApi keyValueApi) {
        super(keyValueApi);
    }

    @RequestMapping(method = RequestMethod.PUT, value = "/config/{WCL}/{RCL}/{timeout}")
    public void configure(@PathVariable final int WCL,
                               @PathVariable final int RCL,
                               @PathVariable final int timeout) {
        ((CoordinatorKeyValueApi)keyValueApi).configure(WCL, RCL, timeout);
    }

    @RequestMapping(method = RequestMethod.PUT, value = "/add-replica/{url}")
    public void addReplica(@PathVariable final String url) {
        if (url.equals("-")) {
            ((CoordinatorKeyValueApi) keyValueApi).addReplica(Application.storageNodeInUse);
        } else {
            ((CoordinatorKeyValueApi) keyValueApi).addReplica(new StorageKeyValueApiHttpClient(url));
        }
    }

}

