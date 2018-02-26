package ru.csc.bdse.app;

import java.util.Set;

/**
 * Phone book record
 *
 * @author alesavin
 */
public interface Record {

    /**
     * Returns literals, associated with Record
     */
    Set<Character> literals();
}