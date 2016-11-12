package com.unidev.polydata;

import com.unidev.polydata.domain.BasicPoly;
import com.unidev.polydata.domain.Poly;
import org.junit.Before;
import org.junit.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;

/**
 * SQLite storage tests
 */
public class SQLiteStorageTest {

    @Before
    public void setup() {
        new File("/tmp/testdb.db").delete();
    }

    @Test
    public void testStorageMigration() throws SQLiteStorageException {
        SQLiteStorage sqLiteStorage = new SQLiteStorage("/tmp/testdb.db");
        sqLiteStorage.migrateStorage();
    }

    @Test
    public void testMetadataFetching() throws SQLiteStorageException {
        SQLiteStorage sqLiteStorage = new SQLiteStorage("/tmp/testdb.db");
        sqLiteStorage.migrateStorage();

        Optional<Poly> tomato = sqLiteStorage.fetchMetadata("tomato");
        assertThat(tomato.isPresent(), is(false));

        BasicPoly basicPoly = new BasicPoly();
        basicPoly._id("tomato");
        basicPoly.put("test_key", "test_value");

        sqLiteStorage.persistMetadata("tomato", basicPoly);


        Optional<Poly> updatedTomato = sqLiteStorage.fetchMetadata("tomato");
        assertThat(updatedTomato.isPresent(), is(true));
        Poly tomatoPoly = updatedTomato.get();

        assertThat(tomatoPoly._id(), is("tomato"));
        assertThat(tomatoPoly.get("test_key"), is("test_value"));


        Poly metadataPoly = sqLiteStorage.metadata();
        assertThat(metadataPoly, is(nullValue()));

        BasicPoly metadataToUpdate = new BasicPoly()._id("meta1");
        sqLiteStorage.metadata(metadataToUpdate);

        Poly changedMetadata = sqLiteStorage.metadata();
        assertThat(changedMetadata, is(notNullValue()));
        assertThat(changedMetadata._id(), is("meta1"));

    }


//        BasicPoly basicPoly = BasicPoly.newPoly("potato");
//        basicPoly.put("value", "tomato");
//
//        sqLiteStorage.save("poly", basicPoly);
//
//        Optional<BasicPoly> polyOptional = sqLiteStorage.fetch("poly", "potato");
//        assertThat(polyOptional.isPresent(), is(true));
//
//        Poly poly = polyOptional.get();
//        assertThat(poly._id(), is("potato"));
//        assertThat(poly.get("value"), is("tomato"));
//
//
//        Optional<BasicPoly> polyOptional2 = sqLiteStorage.fetch("poly", "tomato");
//        assertThat(polyOptional2.isPresent(), is(false));

//    @Test
//    public void testSaveUpdate() throws SQLiteStorageException {
//        SQLiteStorage sqLiteStorage = new SQLiteStorage("/tmp/testdb.db");
//        sqLiteStorage.setPolyMigrators(Arrays.asList(migrator));
//
//        BasicPoly basicPoly = BasicPoly.newPoly("potato");
//        basicPoly.put("value", "tomato");
//
//        sqLiteStorage.save("poly", basicPoly);
//
//        BasicPoly poly = sqLiteStorage.fetch("poly", "potato").get();
//
//        assertThat(poly.get("value"), is("tomato"));
//
//
//        poly.put("value", "another potato");
//        sqLiteStorage.save("poly", poly);
//
//        poly = sqLiteStorage.fetch("poly", "potato").get();
//        assertThat(poly.get("value"), is("another potato"));
//
//    }
//
//    @Test
//    public void testPolyRemoval() throws SQLiteStorageException {
//
//        SQLiteStorage sqLiteStorage = new SQLiteStorage("/tmp/testdb.db");
//        sqLiteStorage.setPolyMigrators(Arrays.asList(migrator));
//
//        BasicPoly basicPoly = BasicPoly.newPoly("potato");
//        basicPoly.put("value", "tomato");
//
//        Optional<BasicPoly> fetch;
//
//        sqLiteStorage.save("poly", basicPoly);
//
//        fetch = sqLiteStorage.fetch("poly", "potato");
//        assertThat(fetch.isPresent(), is(true));
//
//        sqLiteStorage.remove("poly", "potato");
//
//        fetch = sqLiteStorage.fetch("poly", "potato");
//        assertThat(fetch.isPresent(), is(false));
//
//    }
//
//    @Test
//    public void testStatementEvaluation() throws SQLiteStorageException, SQLException {
//        SQLiteStorage sqLiteStorage = new SQLiteStorage("/tmp/testdb.db");
//        sqLiteStorage.setPolyMigrators(Arrays.asList(migrator));
//
//        for(int i = 1;i<=10;i++) {
//            BasicPoly basicPoly = BasicPoly.newPoly("record_" + i);
//            basicPoly.put("value", "" + new Random().nextLong());
//            sqLiteStorage.save("poly", basicPoly);
//        }
//
//        try (Connection connection = sqLiteStorage.openDb()) {
//            PreparedStatement statement = connection.prepareStatement("SELECT * FROM poly;");
//
//            List<BasicPoly> polyList = sqLiteStorage.evaluateStatement(statement);
//
//            assertThat(polyList, not(nullValue()));
//            assertThat(polyList.size(), is(10));
//
//
//            statement = connection.prepareStatement("SELECT _id FROM poly WHERE _id = 'record_3' ");
//            polyList = sqLiteStorage.evaluateStatement(statement);
//
//            assertThat(polyList, not(nullValue()));
//            assertThat(polyList.size(), is(1));
//
//            BasicPoly basicPoly = polyList.get(0);
//
//            assertThat(basicPoly.size(), is(1));
//            assertThat(basicPoly._id(), is("record_3"));
//
//
//            statement = connection.prepareStatement("SELECT _id FROM poly WHERE _id = 'record_666' ");
//            polyList = sqLiteStorage.evaluateStatement(statement);
//
//            assertThat(polyList, not(nullValue()));
//            assertThat(polyList.size(), is(0));
//        }
//
//
//    }


}
