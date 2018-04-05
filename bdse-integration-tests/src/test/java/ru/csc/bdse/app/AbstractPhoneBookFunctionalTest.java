package ru.csc.bdse.app;

import org.junit.Test;

/**
 * Test have to be implemented
 *
 * @author alesavin
 */
public abstract class AbstractPhoneBookFunctionalTest {

/*
    protected abstract PhoneBookApi newPhoneBookApi();
    private PhoneBookApi api = newPhoneBookApi();
*/

    @Test
    public void getFromEmptyBook() {
        // TODO get records from an empty phone book
    }

    @Test
    public void putAndGet() {
        //TODO write some data and read it
    }

    @Test
    public void erasure() {
        //TODO cancel some records
    }

    @Test
    public void update() {
        //TODO update data and put some data twice
    }
}