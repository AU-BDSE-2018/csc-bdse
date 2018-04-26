package ru.csc.bdse.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.DependsOn;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.csc.bdse.kv.KeyValueApi;

@DependsOn("storageNode")
@RestController
@RequestMapping("/partition")
public final class PartitionedKeyValueApiController extends KeyValueApiController {

    public PartitionedKeyValueApiController(@Autowired @Qualifier("partitionNode") final KeyValueApi keyValueApi) {
        super(keyValueApi);
    }

}
