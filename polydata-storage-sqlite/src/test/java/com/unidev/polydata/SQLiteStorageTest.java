package com.unidev.polydata;

import com.unidev.polydata.domain.BasicPoly;
import com.unidev.polydata.domain.Poly;
import org.junit.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;


import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Optional;

/**
 * SQLite storage tests
 */
public class SQLiteStorageTest {

    @Test
    public void testStorageSaveLoad() throws SQLiteStorageException {

        SQLiteStorage sqLiteStorage = new SQLiteStorage("/tmp/testdb.db");
        sqLiteStorage.setPolyMigrators(Arrays.asList(new SQLitePolyMigrator() {
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
        }));

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
