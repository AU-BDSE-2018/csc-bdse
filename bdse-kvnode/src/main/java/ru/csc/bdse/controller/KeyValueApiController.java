package ru.csc.bdse.controller;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

/**
 * Provides HTTP API for the storage unit
 *
 * @author semkagtn
 */
public abstract class KeyValueApiController {

    protected final KeyValueApi keyValueApi;

    public KeyValueApiController(final KeyValueApi keyValueApi) {
        this.keyValueApi = keyValueApi;
    }

    @RequestMapping(method = RequestMethod.PUT, value = "/key-value/{key}")
    public void put(@PathVariable final String key,
                    @RequestBody final byte[] value) {
        keyValueApi.put(key, value);
    }

    @RequestMapping(method = RequestMethod.GET, value = "/key-value/{key}")
    public byte[] get(@PathVariable final String key) {
        return keyValueApi.get(key)
                .orElseThrow(() -> new NoSuchElementException(key));
    }

    @RequestMapping(method = RequestMethod.GET, value = "/key-value")
    public Set<String> getKeys(@RequestParam("prefix") String prefix) {
        return keyValueApi.getKeys(prefix);
    }

    @RequestMapping(method = RequestMethod.DELETE, value = "/key-value/{key}")
    public void delete(@PathVariable final String key) {
        keyValueApi.delete(key);
    }

    @RequestMapping(method = RequestMethod.GET, value = "/info")
    public Set<NodeInfo> getInfo() {
        return keyValueApi.getInfo();
    }

    @RequestMapping(method = RequestMethod.POST, value = "/action/{node}/{action}")
    public void action(@PathVariable final String node,
                       @PathVariable final NodeAction action) { keyValueApi.action(node, action); }

    @ExceptionHandler(NoSuchElementException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public String handle(NoSuchElementException e) {
        return Optional.ofNullable(e.getMessage()).orElse("");
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public String handle(IllegalArgumentException e) {
        return Optional.ofNullable(e.getMessage()).orElse("");
    }
}
