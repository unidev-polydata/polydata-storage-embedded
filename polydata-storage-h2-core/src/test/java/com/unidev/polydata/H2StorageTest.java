package com.unidev.polydata;

import com.unidev.polydata.domain.BasicPoly;
import com.unidev.polydata.domain.Poly;
import java.sql.SQLException;
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
        H2Storage storage = new H2Storage(dbFile.getAbsolutePath());
        storage.migrateStorage();

        BasicPoly poly1 = BasicPoly.newPoly("potato1");
        poly1.put("tomato", "qwe");
        poly1.put(EmbeddedPolyConstants.TAGS_KEY, Arrays.asList("123", "xyz"));

        BasicPoly poly2 = BasicPoly.newPoly("potato2");
        poly2.put("custom", "value");
        poly2.put(EmbeddedPolyConstants.TAGS_KEY, Arrays.asList("tag1", "tag2"));

        try (Connection connection = storage.openDb()) {
            storage.persistPoly(connection, poly1);
            storage.persistPoly(connection, poly2);

            EmbeddedPolyQuery genericQuery = new EmbeddedPolyQuery();
            List<BasicPoly> genericList = storage.listPoly(connection, genericQuery);
            assertThat(genericList.size(), is(2));

            EmbeddedPolyQuery tagQuery = new EmbeddedPolyQuery();
            tagQuery.setTag("tag1");
            List<BasicPoly> listPoly = storage.listPoly(connection, tagQuery);
            assertThat(listPoly.size(), is(1));

            long count = storage.fetchPolyCount(connection, tagQuery);
            assertThat(count, is(1L));

            EmbeddedPolyQuery tagQuery2 = new EmbeddedPolyQuery();
            tagQuery2.setTag("random-tag");

            long count2 = storage.fetchPolyCount(connection, tagQuery2);
            assertThat(count2, is(0L));

            EmbeddedPolyQuery pageQuery = new EmbeddedPolyQuery();
            pageQuery.setPage(0L);
            pageQuery.setItemPerPage(1L);

            listPoly = storage.listPoly(connection, pageQuery);
            assertThat(listPoly.size(), is(1));
            assertThat(listPoly.get(0)._id(), is("potato2"));

            EmbeddedPolyQuery page2Query = new EmbeddedPolyQuery();
            page2Query.setPage(1L);
            page2Query.setItemPerPage(1L);

            listPoly = storage.listPoly(connection, page2Query);
            assertThat(listPoly.size(), is(1));
            assertThat(listPoly.get(0)._id(), is("potato1"));



        }catch (Exception e) {
            throw e;
        }
    }

    @Test
    public void testDBPolyRemoval() throws Exception {
        H2Storage storage = new H2Storage(dbFile.getAbsolutePath());
        storage.migrateStorage();

        BasicPoly poly = BasicPoly.newPoly("potato");
        poly.put("tomato", "qwe");

        try (Connection connection = storage.openDb()) {

            storage.persistPoly(connection, poly);

            Optional<BasicPoly> polyById = storage.fetchPoly(connection, "potato");
            assertThat(polyById.isPresent(), is(true));

            storage.removePoly(connection, "potato");

            Optional<BasicPoly> polyById2 = storage.fetchPoly(connection, "potato");
            assertThat(polyById2.isPresent(), is(false));
        } catch (Exception e) {
            throw e;
        }
    }

    @Test
    public void testTagIndexOperations() throws Exception {
        H2Storage storage = new H2Storage(dbFile.getAbsolutePath());
        storage.migrateStorage();
        storage.migrateTagIndexStorage("tag_index_potato");

        BasicPoly basicPoly = BasicPoly.newPoly();
        basicPoly._id("qwe");
        basicPoly.put("x", "y");

        Connection connection = storage.openDb();

        storage.persistIndexTag(connection, "tag_index_potato", "document_tomato", basicPoly);
        storage.persistIndexTag(connection, "tag_index_potato", "document_tomato", basicPoly);

        long tag_index_count = storage.fetchTagIndexCount(connection, "tag_index_potato");
        assertThat(tag_index_count, is(1L));

        Optional<BasicPoly> dbPoly = storage.fetchTagIndexPoly(connection, "tag_index_potato", "document_tomato");
        assertThat(dbPoly.isPresent(), is(true));
        assertThat(dbPoly.get().fetch("x"), is("y"));

    }

    @Test
    public void testTagOperations() throws Exception {
        H2Storage storage = new H2Storage(dbFile.getAbsolutePath());
        storage.migrateStorage();

        Connection connection = storage.openDb();

        storage.persistTag(connection, BasicPoly.newPoly("test_tag_1"));
        storage.persistTag(connection, BasicPoly.newPoly("test_tag_1"));

        storage.persistTag(connection, BasicPoly.newPoly("test_tag_2"));

        long tagCount = storage.fetchTagCount(connection);
        assertThat(tagCount, is(2L));

        List<BasicPoly> tagList = storage.fetchTags(connection);

        assertThat(tagList, is(notNullValue()));
        assertThat(tagList.size(), is(2));

        Optional<BasicPoly> test_tag_1 = storage.fetchTagPoly(connection, "test_tag_1");
        assertThat(test_tag_1.isPresent(), is(true));
        int count = test_tag_1.get().fetch("_count");
        assertThat(count, is(2));

        Optional<BasicPoly> test_tag_2 = storage.fetchTagPoly(connection, "test_tag_2");
        assertThat(test_tag_2.isPresent(), is(true));
        int count2 = test_tag_2.get().fetch("_count");
        assertThat(count2, is(1));

        Optional<BasicPoly> test_tag_3 = storage.fetchTagPoly(connection, "test_tag_3");
        assertThat(test_tag_3.isPresent(), is(false));



    }

    @Test
    public void testCustomTagStorageUsage() throws SQLException {
        String CUSTOM_TAG_STORAGE = "categories";
        H2Storage storage = new H2Storage(dbFile.getAbsolutePath());
        storage.migrateStorage();
        storage.migrateTagstorage(CUSTOM_TAG_STORAGE);

        try (Connection connection = storage.openDb()) {
            BasicPoly category1 = BasicPoly.newPoly("category1");
            BasicPoly category2 = BasicPoly.newPoly("category2");
            category2.put("x", "y");

            storage.persistTag(connection, CUSTOM_TAG_STORAGE, category1);
            storage.persistTag(connection, CUSTOM_TAG_STORAGE, category1);
            storage.persistTag(connection, CUSTOM_TAG_STORAGE, category2);

            long count = storage.fetchTagCount(connection, CUSTOM_TAG_STORAGE);
            assertThat(count, is(2L));

            List<BasicPoly> list = storage.fetchTags(connection, CUSTOM_TAG_STORAGE);
            assertThat(list.size(), is(2));

            Optional<BasicPoly> poly = storage
                .fetchTagPoly(connection, CUSTOM_TAG_STORAGE, category1._id());

            assertThat(poly.isPresent(), is(true));
            assertThat(poly.get()._id(), is(category1._id()));
            assertThat(poly.get().fetch(EmbeddedPolyConstants.COUNT_KEY), is(2));

            Optional<BasicPoly> tagPoly = storage
                .fetchTagPoly(connection, CUSTOM_TAG_STORAGE, category2._id());
            assertThat(tagPoly.isPresent(), is(true));

        }
    }

}
