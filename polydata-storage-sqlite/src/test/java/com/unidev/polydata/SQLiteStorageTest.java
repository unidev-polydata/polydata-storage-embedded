package com.unidev.polydata;

import com.unidev.polydata.domain.BasicPoly;
import com.unidev.polydata.domain.Poly;
import org.junit.Before;
import org.junit.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;


import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Optional;

/**
 * SQLite storage tests
 */
public class SQLiteStorageTest {

    SQLitePolyMigrator migrator = new SQLitePolyMigrator() {
        @Override
        public boolean canHandle(String poly) {
            return "poly".equalsIgnoreCase(poly);
        }

        @Override
        public void handle(String poly, Connection connection) throws SQLiteStorageException {
            try {
                Statement statement = connection.createStatement();
                statement.executeUpdate("CREATE TABLE IF NOT EXISTS "+poly+" (_id TEXT PRIMARY KEY, value TEXT)");
            } catch (SQLException e) {
                throw new SQLiteStorageException(e);
            }
        }
    };

    @Before
    public void setup() {
        new File("/tmp/testdb.db").delete();
    }

    @Test
    public void testStorageSaveLoad() throws SQLiteStorageException {

        SQLiteStorage sqLiteStorage = new SQLiteStorage("/tmp/testdb.db");
        sqLiteStorage.setPolyMigrators(Arrays.asList(migrator));

        BasicPoly basicPoly = BasicPoly.newPoly("potato");
        basicPoly.put("value", "tomato");

        sqLiteStorage.save("poly", basicPoly);

        Optional<BasicPoly> polyOptional = sqLiteStorage.fetch("poly", "potato");
        assertThat(polyOptional.isPresent(), is(true));

        Poly poly = polyOptional.get();
        assertThat(poly._id(), is("potato"));
        assertThat(poly.get("value"), is("tomato"));


        Optional<BasicPoly> polyOptional2 = sqLiteStorage.fetch("poly", "tomato");
        assertThat(polyOptional2.isPresent(), is(false));
    }

    @Test
    public void testSaveUpdate() throws SQLiteStorageException {
        SQLiteStorage sqLiteStorage = new SQLiteStorage("/tmp/testdb.db");
        sqLiteStorage.setPolyMigrators(Arrays.asList(migrator));

        BasicPoly basicPoly = BasicPoly.newPoly("potato");
        basicPoly.put("value", "tomato");

        sqLiteStorage.save("poly", basicPoly);

        BasicPoly poly = sqLiteStorage.fetch("poly", "potato").get();

        assertThat(poly.get("value"), is("tomato"));


        poly.put("value", "another potato");
        sqLiteStorage.save("poly", poly);

        poly = sqLiteStorage.fetch("poly", "potato").get();
        assertThat(poly.get("value"), is("another potato"));

    }

    @Test
    public void testPolyRemoval() throws SQLiteStorageException {

        SQLiteStorage sqLiteStorage = new SQLiteStorage("/tmp/testdb.db");
        sqLiteStorage.setPolyMigrators(Arrays.asList(migrator));

        BasicPoly basicPoly = BasicPoly.newPoly("potato");
        basicPoly.put("value", "tomato");

        Optional<BasicPoly> fetch;

        sqLiteStorage.save("poly", basicPoly);

        fetch = sqLiteStorage.fetch("poly", "potato");
        assertThat(fetch.isPresent(), is(true));

        sqLiteStorage.remove("poly", "potato");

        fetch = sqLiteStorage.fetch("poly", "potato");
        assertThat(fetch.isPresent(), is(false));

    }


}
