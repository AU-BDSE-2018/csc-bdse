package ru.csc.bdse.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.csc.bdse.kv.KeyValueApi;

@RestController
@RequestMapping("/coordinator")
public final class CoordinatorKeyValueApiController extends KeyValueApiController {

    public CoordinatorKeyValueApiController(@Autowired @Qualifier("coordinatorNode") final KeyValueApi keyValueApi) {
        super(keyValueApi);
    }

}

