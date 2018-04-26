package ru.csc.bdse.kv.client;

import ru.csc.bdse.util.Require;

public class PartitionedKeyValueApiHttpController extends KeyValueApiHttpClient {

    public PartitionedKeyValueApiHttpController(final String baseUrl) {
        super(baseUrl + "/partition");
        Require.nonEmpty(baseUrl, "empty base url");
    }

}
