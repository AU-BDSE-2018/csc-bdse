package ru.csc.bdse.kv.client;

import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import ru.csc.bdse.util.Constants;
import ru.csc.bdse.util.Require;

public final class ControllerKeyValueApiHttpClient extends KeyValueApiHttpClient {

    public ControllerKeyValueApiHttpClient(final String baseUrl) {
        super(baseUrl + "/coordinator");
        Require.nonEmpty(baseUrl, "empty base url");
    }

    public void configure(int WCL, int RCL, int timeout) {
        final String url = baseUrl + "/config/" + WCL + "/" + RCL + "/" + timeout;
        final ResponseEntity<byte[]> responseEntity = request(url, HttpMethod.PUT, Constants.EMPTY_BYTE_ARRAY);
        if (responseEntity.getStatusCode() != HttpStatus.OK) {
            throw new RuntimeException("Response error: " + responseEntity);
        }
    }

    public void addReplica(String replicaUrl) {
        final String url = baseUrl + "/add-replica/" + replicaUrl;
        final ResponseEntity<byte[]> responseEntity = request(url, HttpMethod.PUT, Constants.EMPTY_BYTE_ARRAY);
        if (responseEntity.getStatusCode() != HttpStatus.OK) {
            throw new RuntimeException("Response error: " + responseEntity);
        }
    }

}
