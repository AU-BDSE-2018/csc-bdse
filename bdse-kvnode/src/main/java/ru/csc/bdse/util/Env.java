package ru.csc.bdse.util;

import java.util.Optional;

/**
 * @author semkagtn
 */
public final class Env {

    private Env() {

    }

    public static final String KVNODE_NAME = "KVNODE_NAME";

    public static Optional<String> get(final String name) {
        return Optional.ofNullable(System.getenv(name));
    }
}
