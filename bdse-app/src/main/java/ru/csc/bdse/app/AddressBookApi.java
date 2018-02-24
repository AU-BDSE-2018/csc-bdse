package ru.csc.bdse.app;

import java.util.Iterator;

/**
 * Represents trivial address book operations
 *
 * @author alesavin
 */
public interface AddressBookApi<R extends Record> {

    /**
     * Put record
     */
    void put(R record);

    /**
     * Throw out record
     */
    void erasure(R record);

    /**
     * Get all records associated with literal
     */
    Iterator<R> get(Character literal);
}
