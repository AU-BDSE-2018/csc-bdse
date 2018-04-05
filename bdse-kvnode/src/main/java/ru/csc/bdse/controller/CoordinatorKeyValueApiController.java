package ru.csc.bdse.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.csc.bdse.kv.KeyValueApi;

@RestController
@RequestMapping("/coordinator")
public final class CoordinatorKeyValueApiController extends KeyValueApiController {

    public CoordinatorKeyValueApiController(final KeyValueApi keyValueApi) {
        super(keyValueApi);
    }

}

