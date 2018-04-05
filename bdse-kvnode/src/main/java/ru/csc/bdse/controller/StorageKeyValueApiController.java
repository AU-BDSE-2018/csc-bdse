package ru.csc.bdse.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Primary;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.csc.bdse.kv.KeyValueApi;

@Primary
@RestController
@RequestMapping("/storage")
public final class StorageKeyValueApiController extends KeyValueApiController {

    public StorageKeyValueApiController(@Autowired @Qualifier("storageNode") final KeyValueApi keyValueApi) {
        super(keyValueApi);
    }

}
