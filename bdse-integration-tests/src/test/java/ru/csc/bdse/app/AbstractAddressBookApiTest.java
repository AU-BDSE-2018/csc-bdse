package ru.csc.bdse.app;

import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.KeyValueApiHttpClient;
import ru.csc.bdse.util.Env;

import java.io.File;
import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * Test have to be implemented
 *
 * @author alesavin
 */
public abstract class AbstractAddressBookApiTest {

/*
    protected abstract AddressBookApi newAddressBookApi();
    private AddressBookApi api = newAddressBookApi();
*/

    @Test
    public void getFromEmptyBook() {
        // TODO get records from an empty address book
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


