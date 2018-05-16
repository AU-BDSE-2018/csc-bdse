package ru.csc.bdse.kv.client;

import ru.csc.bdse.util.Require;

public class PartitionedKeyValueApiHttpClient extends KeyValueApiHttpClient {

    public PartitionedKeyValueApiHttpClient(final String baseUrl) {
        super(baseUrl + "/partition");
        Require.nonEmpty(baseUrl, "empty base url");
    }

}
