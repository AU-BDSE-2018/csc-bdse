package ru.csc.bdse.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.csc.bdse.kv.KeyValueApi;

@RestController
@RequestMapping("/storage")
public class StorageKeyValueApiController extends KeyValueApiController {

    public StorageKeyValueApiController(final KeyValueApi keyValueApi) {
        super(keyValueApi);
    }

}
