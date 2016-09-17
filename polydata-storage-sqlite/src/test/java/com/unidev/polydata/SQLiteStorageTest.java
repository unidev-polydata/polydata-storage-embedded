package com.unidev.polydata;

import com.unidev.polydata.domain.BasicPoly;
import com.unidev.polydata.domain.Poly;
import org.junit.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;


import java.util.Optional;

/**
 * SQLite storage tests
 */
public class SQLiteStorageTest {

    @Test
    public void testStorageSaveLoad() throws SQLiteStorageException {

        SQLiteStorage sqLiteStorage = new SQLiteStorage("/tmp/testdb.db");

        BasicPoly basicPoly = BasicPoly.newPoly("potato");
        basicPoly.put("value", "tomato");


        sqLiteStorage.save("poly", basicPoly);

        Optional<Poly> polyOptional = sqLiteStorage.fetch("poly", "potato");
        assertThat(polyOptional.isPresent(), is(true));

        Poly poly = polyOptional.get();
        assertThat(poly._id(), is("potato"));
        assertThat(poly.get("value"), is("tomato"));


        Optional<Poly> polyOptional2 = sqLiteStorage.fetch("poly", "tomato");
        assertThat(polyOptional2.isPresent(), is(false));
    }
}
