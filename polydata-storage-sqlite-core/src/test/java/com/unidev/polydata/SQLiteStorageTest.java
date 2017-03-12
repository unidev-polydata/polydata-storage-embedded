package com.unidev.polydata;

import com.unidev.polydata.domain.BasicPoly;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;

/**
 * SQLite storage tests
 */
public class SQLiteStorageTest {

    File dbFile;

    @Before
    public void setup() {
        dbFile = new File("/tmp/testdb.db");
        dbFile.delete();
    }

    @Test
    public void testStorageMigration() throws SQLiteStorageException {
        SQLiteStorage sqLiteStorage = new SQLiteStorage(dbFile.getAbsolutePath());
        sqLiteStorage.migrateStorage();
    }

    @Test
    public void testStoragePolyStorage() {
        SQLiteStorage sqLiteStorage = new SQLiteStorage(dbFile.getAbsolutePath());
        sqLiteStorage.migrateStorage();

        BasicPoly poly = BasicPoly.newPoly("potato");

        try (Connection connection = sqLiteStorage.openDb()) {
            sqLiteStorage.persistPoly(connection, poly);
        }catch (Exception e) {

        }

    }
//
//    @Test
//    public void testMetadataFetching() throws SQLiteStorageException {
//        SQLiteStorage sqLiteStorage = new SQLiteStorage("/tmp/testdb.db");
//        sqLiteStorage.migrateStorage();
//
//        Optional<Poly> tomato = sqLiteStorage.fetchMetadata("tomato");
//        assertThat(tomato.isPresent(), is(false));
//
//        BasicPoly basicPoly = new BasicPoly();
//        basicPoly._id("tomato");
//        basicPoly.put("test_key", "test_value");
//
//        sqLiteStorage.persistMetadata("tomato", basicPoly);
//
//
//        Optional<Poly> updatedTomato = sqLiteStorage.fetchMetadata("tomato");
//        assertThat(updatedTomato.isPresent(), is(true));
//        Poly tomatoPoly = updatedTomato.get();
//
//        assertThat(tomatoPoly._id(), is("tomato"));
//        assertThat(tomatoPoly.get("test_key"), is("test_value"));
//
//
//        Poly metadataPoly = sqLiteStorage.metadata();
//        assertThat(metadataPoly, is(nullValue()));
//
//        BasicPoly metadataToUpdate = new BasicPoly()._id("meta1");
//        sqLiteStorage.metadata(metadataToUpdate);
//
//        Poly changedMetadata = sqLiteStorage.metadata();
//        assertThat(changedMetadata, is(notNullValue()));
//        assertThat(changedMetadata._id(), is("meta1"));
//
//    }
//
//
//    @Test
//    public void testSaveUpdate() throws SQLiteStorageException {
//        SQLiteStorage sqLiteStorage = new SQLiteStorage("/tmp/testdb.db");
//        sqLiteStorage.migrateStorage();
//
//
//        BasicPoly basicPoly = BasicPoly.newPoly("potato");
//        basicPoly.put("value", "tomato");
//
//        Poly poly = sqLiteStorage.fetchById("potato");
//        assertThat(poly, is(nullValue()));
//
//        sqLiteStorage.persist(basicPoly);
//
//        Poly dbPoly = sqLiteStorage.fetchById("potato");
//        assertThat(dbPoly, is(notNullValue()));
//        assertThat(dbPoly.get("value"), is("tomato"));
//
//
//        BasicPoly updatePoly = BasicPoly.newPoly("potato");
//        updatePoly.put("value", "tomato2");
//        sqLiteStorage.persist(updatePoly);
//
//        Poly updatedPoly = sqLiteStorage.fetchById("potato");
//        assertThat(updatedPoly, is(notNullValue()));
//        assertThat(updatedPoly.get("value"), is("tomato2"));
//    }
//
//    @Test
//    public void testPolyRemoval() throws SQLiteStorageException {
//
//        SQLiteStorage sqLiteStorage = new SQLiteStorage("/tmp/testdb.db");
//        sqLiteStorage.migrateStorage();
//
//
//        boolean missingRemoval = sqLiteStorage.remove("potato");
//        assertThat(missingRemoval, is(false));
//
//        BasicPoly basicPoly = BasicPoly.newPoly("potato");
//        basicPoly.put("value", "tomato");
//
//        sqLiteStorage.persist(basicPoly);
//
//        boolean removalResult = sqLiteStorage.remove("potato");
//        assertThat(removalResult, is(true));
//
//        boolean missingRemoval2 = sqLiteStorage.remove("potato");
//        assertThat(missingRemoval2, is(false));
//    }
//
//    @Test
//    public void testPolyListing() {
//        SQLiteStorage sqLiteStorage = new SQLiteStorage("/tmp/testdb.db");
//        sqLiteStorage.migrateStorage();
//
//        assertThat(sqLiteStorage.size(), is(0L));
//
//        Collection<? extends Poly> list = sqLiteStorage.list();
//        assertThat(list.isEmpty(), is(true));
//
//        BasicPoly basicPoly = BasicPoly.newPoly("potato");
//        basicPoly.put("value", "tomato");
//
//        sqLiteStorage.persist(basicPoly);
//
//        assertThat(sqLiteStorage.size(), is(1L));
//        Collection<? extends Poly> records = sqLiteStorage.list();
//        assertThat(records.isEmpty(), is(false));
//        assertThat(records.size(), is(1));
//
//        Poly poly = records.iterator().next();
//        assertThat(poly, is(not(nullValue())));
//        assertThat(poly._id(), is("potato"));
//        assertThat(poly.get("value"), is("tomato"));
//
//
//    }

}
