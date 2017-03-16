package com.unidev.polydata;

import com.unidev.polydata.domain.BasicPoly;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

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
    public void testPersistingPoly() throws SQLException {
        SQLiteStorage sqLiteStorage = new SQLiteStorage(dbFile.getAbsolutePath());
        sqLiteStorage.migrateStorage();

        BasicPoly poly = BasicPoly.newPoly("potato");
        poly.put("tomato", "qwe");
        try (Connection connection = sqLiteStorage.openDb()) {

            Optional<BasicPoly> optinalPoly = sqLiteStorage.fetchPoly(connection, poly._id());
            assertThat(optinalPoly.isPresent(), is(false));

            sqLiteStorage.persistPoly(connection, poly);

            Optional<BasicPoly> dbPoly = sqLiteStorage.fetchPoly(connection, poly._id());
            assertThat(dbPoly.isPresent(), is(true));

            assertThat(dbPoly.get()._id(), is("potato"));
            assertThat(dbPoly.get().fetch("tomato"), is("qwe"));

            poly.put("new-field", "987");
            poly.put("tomato", "000");

            sqLiteStorage.persistPoly(connection, poly);

            Optional<BasicPoly> dbPoly2 = sqLiteStorage.fetchPoly(connection, poly._id());
            assertThat(dbPoly2.isPresent(), is(true));

            assertThat(dbPoly2.get()._id(), is("potato"));
            assertThat(dbPoly2.get().fetch("tomato"), is("000"));
            assertThat(dbPoly2.get().fetch("new-field"), is("987"));
        }catch (Exception e) {
            throw e;
        }

    }

    @Test
    public void testPolyRemoval() throws SQLException {
        SQLiteStorage sqLiteStorage = new SQLiteStorage(dbFile.getAbsolutePath());
        sqLiteStorage.migrateStorage();

        BasicPoly poly = BasicPoly.newPoly("potato");
        poly.put("tomato", "qwe");

        try (Connection connection = sqLiteStorage.openDb()) {

            sqLiteStorage.persistPoly(connection, poly);

            Optional<BasicPoly> polyById = sqLiteStorage.fetchPoly(connection, "potato");
            assertThat(polyById.isPresent(), is(true));

            sqLiteStorage.removePoly(connection, "potato");

            Optional<BasicPoly> polyById2 = sqLiteStorage.fetchPoly(connection, "potato");
            assertThat(polyById2.isPresent(), is(false));
        } catch (Exception e) {
            throw e;
        }
    }

    @Test
    public void testTagIndexOperations() throws SQLException {
        SQLiteStorage sqLiteStorage = new SQLiteStorage(dbFile.getAbsolutePath());
        sqLiteStorage.migrateStorage();

        BasicPoly basicPoly = BasicPoly.newPoly();
        basicPoly._id("qwe");

        Connection connection = sqLiteStorage.openDb();

        sqLiteStorage.persistIndexTag(connection, "tag_index_potato", "document_tomato", basicPoly);
        sqLiteStorage.persistIndexTag(connection, "tag_index_potato", "document_tomato", basicPoly);

        Optional<Long> tag_index_count = sqLiteStorage.fetchTagIndexCount(connection, "tag_index_potato");
        assertThat(tag_index_count.isPresent(), is(true));
        assertThat(tag_index_count.get()  == 1L, is(true));
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
