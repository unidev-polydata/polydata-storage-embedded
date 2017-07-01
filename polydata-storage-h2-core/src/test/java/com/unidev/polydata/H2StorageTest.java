package com.unidev.polydata;

import com.unidev.polydata.domain.BasicPoly;
import com.unidev.polydata.domain.Poly;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.util.*;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.core.Is.*;

/**
 * SQLite storage tests
 */
public class H2StorageTest {

    File dbFile;

    @Before
    public void setup() {
        dbFile = new File("/tmp/testdb");
        dbFile.delete();

        File realDbFile = new File("/tmp/testdb.mv.db");
        realDbFile.delete();
    }

    @Test
    public void testStorageMigration() throws EmbeddedStorageException {
        H2Storage h2Storage = new H2Storage(dbFile.getAbsolutePath());
        h2Storage.migrateStorage();
    }

    @Test
    public void testPersistingPoly() throws Exception {
        H2Storage h2Storage = new H2Storage(dbFile.getAbsolutePath());
        h2Storage.migrateStorage();

        try (Connection connection = h2Storage.openDb()) {

            BasicPoly poly = BasicPoly.newPoly("potato");
            poly.put("tomato", "qwe");
            poly.put(EmbeddedPolyConstants.TAGS_KEY, Arrays.asList("123", "xyz"));

            BasicPoly poly2 = BasicPoly.newPoly("tomato");
            poly2.put("tomato", "qwe");
            poly2.put(EmbeddedPolyConstants.TAGS_KEY, Arrays.asList("123", "xyz"));

            Optional<BasicPoly> optinalPoly = h2Storage.fetchPoly(connection, poly._id());
            assertThat(optinalPoly.isPresent(), is(false));

            h2Storage.persistPoly(connection, poly);
            h2Storage.persistPoly(connection, poly2);

            Optional<BasicPoly> dbPoly = h2Storage.fetchPoly(connection, poly._id());
            assertThat(dbPoly.isPresent(), is(true));

            assertThat(dbPoly.get()._id(), is("potato"));
            assertThat(dbPoly.get().fetch("tomato"), is("qwe"));

            poly.put("new-field", "987");
            poly.put("tomato", "000");

            h2Storage.persistPoly(connection, poly);

            Optional<BasicPoly> dbPoly2 = h2Storage.fetchPoly(connection, poly._id());
            assertThat(dbPoly2.isPresent(), is(true));

            assertThat(dbPoly2.get()._id(), is("potato"));
            assertThat(dbPoly2.get().fetch("tomato"), is("000"));
            assertThat(dbPoly2.get().fetch("new-field"), is("987"));

            // batch poly fetching

            Collection<Optional<BasicPoly>> polys = h2Storage.fetchPolys(connection, Arrays.asList("potato", "tomato", "random-id"));
            assertThat(polys.size(), is(3));

            Map<String, Optional<BasicPoly>> fetchPolyMap = h2Storage.fetchPolyMap(connection, Arrays.asList("potato", "tomato", "random-id"));
            assertThat(fetchPolyMap.size(), is(3));

            assertThat(fetchPolyMap.get("random-id").isPresent(), is(false));
            assertThat(fetchPolyMap.get("potato").isPresent(), is(true));
            assertThat(fetchPolyMap.get("potato").get()._id(), is("potato"));


        }catch (Exception e) {
            throw e;
        }

    }

    @Test
    public void testPolyQuery() throws Exception {
        H2Storage sqLiteStorage = new H2Storage(dbFile.getAbsolutePath());
        sqLiteStorage.migrateStorage();

        BasicPoly poly1 = BasicPoly.newPoly("potato1");
        poly1.put("tomato", "qwe");
        poly1.put(EmbeddedPolyConstants.TAGS_KEY, Arrays.asList("123", "xyz"));

        BasicPoly poly2 = BasicPoly.newPoly("potato2");
        poly2.put("custom", "value");
        poly2.put(EmbeddedPolyConstants.TAGS_KEY, Arrays.asList("tag1", "tag2"));

        try (Connection connection = sqLiteStorage.openDb()) {
            sqLiteStorage.persistPoly(connection, poly1);
            sqLiteStorage.persistPoly(connection, poly2);

            EmbeddedPolyQuery genericQuery = new EmbeddedPolyQuery();
            List<BasicPoly> genericList = sqLiteStorage.listPoly(connection, genericQuery);
            assertThat(genericList.size(), is(2));

            EmbeddedPolyQuery tagQuery = new EmbeddedPolyQuery();
            tagQuery.setTag("tag1");
            List<BasicPoly> listPoly = sqLiteStorage.listPoly(connection, tagQuery);
            assertThat(listPoly.size(), is(1));

            long count = sqLiteStorage.fetchPolyCount(connection, tagQuery);
            assertThat(count, is(1L));

            EmbeddedPolyQuery tagQuery2 = new EmbeddedPolyQuery();
            tagQuery2.setTag("random-tag");

            long count2 = sqLiteStorage.fetchPolyCount(connection, tagQuery2);
            assertThat(count2, is(0L));

            EmbeddedPolyQuery pageQuery = new EmbeddedPolyQuery();
            pageQuery.setPage(0L);
            pageQuery.setItemPerPage(1L);

            listPoly = sqLiteStorage.listPoly(connection, pageQuery);
            assertThat(listPoly.size(), is(1));
            assertThat(listPoly.get(0)._id(), is("potato2"));

            EmbeddedPolyQuery page2Query = new EmbeddedPolyQuery();
            page2Query.setPage(1L);
            page2Query.setItemPerPage(1L);

            listPoly = sqLiteStorage.listPoly(connection, page2Query);
            assertThat(listPoly.size(), is(1));
            assertThat(listPoly.get(0)._id(), is("potato1"));



        }catch (Exception e) {
            throw e;
        }
    }

    @Test
    public void testDBPolyRemoval() throws Exception {
        H2Storage sqLiteStorage = new H2Storage(dbFile.getAbsolutePath());
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
    public void testTagIndexOperations() throws Exception {
        H2Storage sqLiteStorage = new H2Storage(dbFile.getAbsolutePath());
        sqLiteStorage.migrateStorage();
        sqLiteStorage.migrateTagIndexStorage("tag_index_potato");

        BasicPoly basicPoly = BasicPoly.newPoly();
        basicPoly._id("qwe");
        basicPoly.put("x", "y");

        Connection connection = sqLiteStorage.openDb();

        sqLiteStorage.persistIndexTag(connection, "tag_index_potato", "document_tomato", basicPoly);
        sqLiteStorage.persistIndexTag(connection, "tag_index_potato", "document_tomato", basicPoly);

        long tag_index_count = sqLiteStorage.fetchTagIndexCount(connection, "tag_index_potato");
        assertThat(tag_index_count, is(1L));

        Optional<BasicPoly> dbPoly = sqLiteStorage.fetchTagIndexPoly(connection, "tag_index_potato", "document_tomato");
        assertThat(dbPoly.isPresent(), is(true));
        assertThat(dbPoly.get().fetch("x"), is("y"));

    }

    @Test
    public void testTagOperations() throws Exception {
        H2Storage sqLiteStorage = new H2Storage(dbFile.getAbsolutePath());
        sqLiteStorage.migrateStorage();

        Connection connection = sqLiteStorage.openDb();

        sqLiteStorage.persistTag(connection, BasicPoly.newPoly("test_tag_1"));
        sqLiteStorage.persistTag(connection, BasicPoly.newPoly("test_tag_1"));

        sqLiteStorage.persistTag(connection, BasicPoly.newPoly("test_tag_2"));

        long tagCount = sqLiteStorage.fetchTagCount(connection);
        assertThat(tagCount, is(2L));

        List<BasicPoly> tagList = sqLiteStorage.fetchTags(connection);

        assertThat(tagList, is(notNullValue()));
        assertThat(tagList.size(), is(2));

        Optional<BasicPoly> test_tag_1 = sqLiteStorage.fetchTagPoly(connection, "test_tag_1");
        assertThat(test_tag_1.isPresent(), is(true));
        int count = test_tag_1.get().fetch("_count");
        assertThat(count, is(2));

        Optional<BasicPoly> test_tag_2 = sqLiteStorage.fetchTagPoly(connection, "test_tag_2");
        assertThat(test_tag_2.isPresent(), is(true));
        int count2 = test_tag_2.get().fetch("_count");
        assertThat(count2, is(1));

        Optional<BasicPoly> test_tag_3 = sqLiteStorage.fetchTagPoly(connection, "test_tag_3");
        assertThat(test_tag_3.isPresent(), is(false));



    }


//    @Test
//    public void testMetadataFetching() throws EmbeddedStorageException {
//        H2Storage sqLiteStorage = new H2Storage("/tmp/testdb.db");
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
//    public void testSaveUpdate() throws EmbeddedStorageException {
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
//    public void testPolyRemoval() throws EmbeddedStorageException {
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