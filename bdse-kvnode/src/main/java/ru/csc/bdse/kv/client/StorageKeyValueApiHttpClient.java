package ru.csc.bdse.kv.client;

import ru.csc.bdse.util.Require;

public final class StorageKeyValueApiHttpClient extends KeyValueApiHttpClient {

    public StorageKeyValueApiHttpClient(final String baseUrl) {
        super(baseUrl + "/storage");
        Require.nonEmpty(baseUrl, "empty base url");
    }

}
