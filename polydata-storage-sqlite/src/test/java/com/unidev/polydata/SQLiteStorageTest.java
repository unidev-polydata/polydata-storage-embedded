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


    @Test
    public void testSaveUpdate() throws SQLiteStorageException {
        SQLiteStorage sqLiteStorage = new SQLiteStorage("/tmp/testdb.db");
        sqLiteStorage.migrateStorage();


        BasicPoly basicPoly = BasicPoly.newPoly("potato");
        basicPoly.put("value", "tomato");

        Poly poly = sqLiteStorage.fetchById("potato");
        assertThat(poly, is(nullValue()));

        sqLiteStorage.persist(basicPoly);

        Poly dbPoly = sqLiteStorage.fetchById("potato");
        assertThat(dbPoly, is(notNullValue()));
        assertThat(dbPoly.get("value"), is("tomato"));


        BasicPoly updatePoly = BasicPoly.newPoly("potato");
        updatePoly.put("value", "tomato2");
        sqLiteStorage.persist(updatePoly);

        Poly updatedPoly = sqLiteStorage.fetchById("potato");
        assertThat(updatedPoly, is(notNullValue()));
        assertThat(updatedPoly.get("value"), is("tomato2"));



    }


    @Test
    public void testPolyRemoval() throws SQLiteStorageException {

        SQLiteStorage sqLiteStorage = new SQLiteStorage("/tmp/testdb.db");
        sqLiteStorage.migrateStorage();


        boolean missingRemoval = sqLiteStorage.remove("potato");
        assertThat(missingRemoval, is(false));

        BasicPoly basicPoly = BasicPoly.newPoly("potato");
        basicPoly.put("value", "tomato");

        sqLiteStorage.persist(basicPoly);

        boolean removalResult = sqLiteStorage.remove("potato");
        assertThat(removalResult, is(true));

        boolean missingRemoval2 = sqLiteStorage.remove("potato");
        assertThat(missingRemoval2, is(false));

    }
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
