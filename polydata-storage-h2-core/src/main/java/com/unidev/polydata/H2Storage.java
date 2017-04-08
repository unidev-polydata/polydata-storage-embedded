package com.unidev.polydata;


import com.unidev.polydata.domain.BasicPoly;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class H2Storage extends AbstractEmbeddedStorage {

    private static Logger LOG = LoggerFactory.getLogger(H2Storage.class);

    static {
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            LOG.error("Failed to load SQLite driver", e);
        }
    }

    public H2Storage(String dbFile) {
        super(dbFile);
    }

    @Override
    public Connection openDb() {
        try {
            return DriverManager.getConnection("jdbc:h2:" + dbFile);
        } catch (SQLException e) {
            LOG.warn("Failed to open db {}", dbFile, e);
            throw new EmbeddedStorageException(e);
        }
    }

    @Override
    public void migrateStorage() {
        Flyway flyway = new Flyway();
        flyway.setDataSource("jdbc:h2:" + dbFile, null, null);
        flyway.setOutOfOrder(true);
        flyway.setLocations("db/polystorage");
        flyway.migrate();
    }

    @Override
    public Optional<BasicPoly> fetchPoly(Connection connection, String id) {
        return null;
    }

    @Override
    public Map<String, Optional<BasicPoly>> fetchPolyMap(Connection connection, Collection<String> polyIds) {
        return null;
    }

    @Override
    public Collection<Optional<BasicPoly>> fetchPolys(Connection connection, Collection<String> polyIds) {
        return null;
    }

    @Override
    public BasicPoly persistPoly(Connection connection, BasicPoly poly) {
        return null;
    }

    @Override
    public long fetchPolyCount(Connection connection, EmbeddedPolyQuery polyQuery) {
        return 0;
    }

    @Override
    public List<BasicPoly> listPoly(Connection connection, EmbeddedPolyQuery polyQuery) {
        return null;
    }

    @Override
    public boolean removePoly(Connection connection, String polyId) {
        return false;
    }

    @Override
    public BasicPoly persistTag(Connection connection, BasicPoly tagPoly) {
        return null;
    }

    @Override
    public List<BasicPoly> fetchTags(Connection connection) {
        return null;
    }

    @Override
    public Optional<BasicPoly> fetchTagPoly(Connection connection, String id) {
        return null;
    }

    @Override
    public long fetchTagCount(Connection connection) {
        return 0;
    }

    @Override
    public BasicPoly persistIndexTag(Connection connection, String tagIndex, String documentId, BasicPoly data) {
        return null;
    }

    @Override
    public List<BasicPoly> fetchTagIndex(Connection connection, String tagIndex) {
        return null;
    }

    @Override
    public Optional<BasicPoly> fetchTagIndexPoly(Connection connection, String tagIndex, String documentId) {
        return null;
    }

    @Override
    public Optional<BasicPoly> fetchTagIndexPolyByTag(Connection connection, String tagIndex, String tag) {
        return null;
    }

    @Override
    public long fetchTagIndexCount(Connection connection, String tagIndex) {
        return 0;
    }

    @Override
    public Long fetchPolyCount(Connection connection, String table) {
        return null;
    }

    @Override
    public Optional<BasicPoly> fetchRawPoly(Connection connection, String table, String id) {
        return null;
    }

    @Override
    public boolean removeRawPoly(Connection connection, String table, String id) {
        return false;
    }
}
