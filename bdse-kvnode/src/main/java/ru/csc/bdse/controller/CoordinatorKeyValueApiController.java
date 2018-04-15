package ru.csc.bdse.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.DependsOn;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.csc.bdse.kv.KeyValueApi;

// We must create coordinator controller only after we've created local storage controller
// FIXME I don't know why, but @Order and @Primary don't affect their creation ordering. So I just do this hack
@DependsOn("storageNode")
@RestController
@RequestMapping("/coordinator")
public final class CoordinatorKeyValueApiController extends KeyValueApiController {

    public CoordinatorKeyValueApiController(@Autowired @Qualifier("coordinatorNode") final KeyValueApi keyValueApi) {
        super(keyValueApi);
    }

}
