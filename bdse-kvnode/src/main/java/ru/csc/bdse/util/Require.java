package ru.csc.bdse.util;

/**
 * @author semkagtn
 */
public final class Require {

    private Require() {

    }

    public static void nonNull(final Object object, final String errorMsg) {
        if (object == null) throw new IllegalArgumentException(errorMsg);
    }

    public static void nonEmpty(final String string, final String errorMsg) {
        if (string == null || string.isEmpty()) throw new IllegalArgumentException(errorMsg);
    }
}
