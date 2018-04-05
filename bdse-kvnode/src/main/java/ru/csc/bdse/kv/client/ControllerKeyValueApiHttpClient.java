package ru.csc.bdse.kv.client;

import ru.csc.bdse.util.Require;

public final class ControllerKeyValueApiHttpClient extends KeyValueApiHttpClient {

    public ControllerKeyValueApiHttpClient(final String baseUrl) {
        super(baseUrl + "/controller");
        Require.nonEmpty(baseUrl, "empty base url");
    }

}
