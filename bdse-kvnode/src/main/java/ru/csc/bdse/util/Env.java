package ru.csc.bdse.util;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * @author semkagtn
 */
public final class Env {

    private Env() {

    }

    public static final String KVNODE_NAME = "KVNODE_NAME";
    public static final String NETWORK_NAME = "NETWORK_NAME";
    public static final String WCL = "WCL";
    public static final String RCL = "RCL";
    public static final String TIMEOUT = "TIMEOUT";
    public static final String REPLICS = "REPLICS";

    public static Optional<String> get(final String name) {
        return Optional.ofNullable(System.getenv(name));
    }

    public static Optional<Integer> getInt(final String name) {
        final Optional<String> str = get(name);
        return str.map(Integer::parseInt);
    }

    public static Optional<List<String>> getList(final String name) {
        final Optional<String> listStr = get(name);
        return listStr.map(l -> Arrays.asList(l.split(",")));
    }

}
