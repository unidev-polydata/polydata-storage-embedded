package com.unidev.polydata;


import com.unidev.polydata.domain.BasicPoly;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

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
    public BasicPoly persistIndexTag(Connection connection, String tagIndex, String documentId, BasicPoly data) {
        return null;
    }
}
