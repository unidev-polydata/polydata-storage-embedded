package com.unidev.polydata;

import com.unidev.polydata.domain.BasicPoly;
import com.unidev.polydata.domain.BasicPolyList;
import com.unidev.polydata.domain.Poly;
import com.unidev.polydata.domain.PolyList;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
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

    private SQLiteStorage fetchStorage() {
        SQLiteStorage sqLiteStorage = new SQLiteStorage(dbFile.getAbsolutePath());
        sqLiteStorage.migrateStorage();
        return sqLiteStorage;
    }

    @Test
    public void testStorageMigration() throws EmbeddedStorageException {
        fetchStorage();
    }

    @Test
    public void testMetadataOperations() {
        SQLiteStorage sqLiteStorage = fetchStorage();

        Optional<Poly> metadata = sqLiteStorage.metadata("main");
        assertThat(metadata.isPresent(), is(false));

        sqLiteStorage.persistMetadata("main", BasicPoly.newPoly("main").with("value", "tomato"));

        Optional<Poly> metadata2 = sqLiteStorage.metadata("main");
        assertThat(metadata2.isPresent(), is(true));
        assertThat(metadata2.get()._id(), is("main"));
    }

    @Test
    public void testPolyPersisting() {
        SQLiteStorage sqLiteStorage = fetchStorage();

        BasicPoly poly = BasicPoly.newPoly("test");

        poly.put(EmbeddedPolyConstants.TAGS_KEY, Arrays.asList("tag1", "tag2"));
        poly.put("tomato", "potato");

        sqLiteStorage.persist("main", poly);

        Optional<Poly> fetchById = sqLiteStorage.fetchById("main", "test");
        assertThat(fetchById.isPresent(), is(true));

        assertThat(fetchById.get()._id(), is("test"));
        Poly polyById = fetchById.get();

        assertThat(polyById.fetch("tomato"), is("potato"));
        assertThat(polyById.fetch("randomText"), is(nullValue()));

        List<String> tags = polyById.fetch(EmbeddedPolyConstants.TAGS_KEY);
        assertThat(tags, is(notNullValue()));
        assertThat(tags.contains("tag1"), is(true));
    }

    @Test
    public void testPolyRemoval() {
        SQLiteStorage sqLiteStorage = fetchStorage();
        BasicPoly poly = BasicPoly.newPoly("test");
        sqLiteStorage.persist("main", poly);

        boolean removePoly = sqLiteStorage.removePoly("main", "test");
        assertThat(removePoly, is(true));

        Optional<Poly> fetchById2 = sqLiteStorage.fetchById("main", "test");
        assertThat(fetchById2.isPresent(), is(false));
    }

    @Test
    public void testPolyUpdate() {
        SQLiteStorage sqLiteStorage = fetchStorage();

        BasicPoly poly = BasicPoly.newPoly("test");
        poly.put("1", "2");
        poly.put(EmbeddedPolyConstants.TAGS_KEY, Arrays.asList("tag1", "tag2"));

        sqLiteStorage.persist("main", poly);

        Optional<Poly> fetchById = sqLiteStorage.fetchById("main", "test");
        assertThat(fetchById.get().fetch("1"), is("2"));

        poly.put("1", "3");
        sqLiteStorage.persist("main", poly);

        Optional<Poly> fetchById2 = sqLiteStorage.fetchById("main", "test");
        assertThat(fetchById2.get().fetch("1"), is("3"));

    }

    @Test
    public void testTagPersisting() {
        SQLiteStorage sqLiteStorage = fetchStorage();

        BasicPoly poly = BasicPoly.newPoly("id1");
        poly.put("1", "2");
        poly.put(EmbeddedPolyConstants.TAGS_KEY, Arrays.asList("tag1", "tag2"));
        sqLiteStorage.persist("main", poly);


        BasicPoly poly2 = BasicPoly.newPoly("id2");
        poly2.put("1", "2");
        poly2.put(EmbeddedPolyConstants.TAGS_KEY, Arrays.asList("tag1", "tag2", "tag3"));
        sqLiteStorage.persist("main", poly2);

        BasicPoly tags = sqLiteStorage.fetchTags("main");

        assertThat(tags.fetch("tag3"), is(1));
        assertThat(tags.fetch("tag2"), is(2));
        assertThat(tags.fetch("tag1"), is(2));

    }

    @Test
    public void testQuery() throws InterruptedException {
        SQLiteStorage sqLiteStorage = fetchStorage();

        for(int i = 0;i<10;i++) {
            BasicPoly poly = BasicPoly.newPoly("id_" + i);
            poly.put("1", "2");
            poly.put(EmbeddedPolyConstants.TAGS_KEY, Arrays.asList("tag" + i, "tag" + (i + 1)));
            sqLiteStorage.persist("main", poly);
            Thread.sleep(1000);
        }

        assertThat(sqLiteStorage.fetchPolyCount("main"), is(10L));

        EmbeddedPolyQuery embeddedPolyQuery = EmbeddedPolyQuery.builder()
                .page(0L)
                .itemPerPage(2L)
                .build();

        PolyList list = sqLiteStorage.query("main", embeddedPolyQuery);

        assertThat(list, is(notNullValue()));
        assertThat(list.list(), is(notNullValue()));
        assertThat(list.list().size(), is(2));

        BasicPoly basicPoly = (BasicPoly) list.list().get(0);
        assertThat(basicPoly._id(), is("id_9"));

        EmbeddedPolyQuery embeddedPolyQuery2 = EmbeddedPolyQuery.builder()
                .page(110L)
                .itemPerPage(2L)
                .build();

        PolyList list2 = sqLiteStorage.query("main", embeddedPolyQuery2);
        assertThat(list2.list().size(), is(0));

    }

    @Test
    public void testTagQuery() throws InterruptedException {
        SQLiteStorage sqLiteStorage = fetchStorage();

        for(int i = 0;i<10;i++) {
            BasicPoly poly = BasicPoly.newPoly("id_" + i);
            poly.put("1", "2");
            poly.put("iteration", i);
            poly.put(EmbeddedPolyConstants.TAGS_KEY, Arrays.asList("tag" + i, "tag" + (i + 1)));
            sqLiteStorage.persist("main", poly);
            Thread.sleep(1000);
        }

        assertThat(sqLiteStorage.fetchPolyCount("main"), is(10L));

        EmbeddedPolyQuery embeddedPolyQuery = EmbeddedPolyQuery.builder()
                .page(0L)
                .tag("tag0")
                .itemPerPage(10L)
                .build();

        PolyList list = sqLiteStorage.queryIndex("main", embeddedPolyQuery);

        assertThat(list, is(notNullValue()));
        assertThat(list.list(), is(notNullValue()));
        assertThat(list.list().size(), is(1));

        BasicPoly basicPoly = (BasicPoly) list.list().get(0);
        assertThat(basicPoly._id(), is("id_0"));
        assertThat(basicPoly.fetch("iteration"), is(0));
        assertThat(basicPoly.fetch("1"), is("2"));

        EmbeddedPolyQuery embeddedPolyQuery2 = EmbeddedPolyQuery.builder()
                .page(0L)
                .tag("tag1")
                .itemPerPage(10L)
                .build();

        PolyList list2 = sqLiteStorage.queryIndex("main", embeddedPolyQuery2);
        assertThat(list2.list().size(), is(2));

        BasicPoly basicPoly2 = (BasicPoly) list2.list().get(0);
        assertThat(basicPoly2._id(), is("id_1"));
        assertThat(basicPoly2.fetch("iteration"), is(1));

        BasicPoly basicPoly3 = (BasicPoly) list2.list().get(1);
        assertThat(basicPoly3._id(), is("id_0"));
    }

}
