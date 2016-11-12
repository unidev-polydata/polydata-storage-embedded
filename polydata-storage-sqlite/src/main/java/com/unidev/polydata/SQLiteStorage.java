package com.unidev.polydata;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.unidev.polydata.domain.BasicPoly;
import com.unidev.polydata.domain.Poly;
import com.unidev.polydata.storage.ChangablePolyStorage;
import org.flywaydb.core.Flyway;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Named polydata storage,
 * Each poly storage will be dedicated table
 */
public class SQLiteStorage implements ChangablePolyStorage {
    private static Logger LOG = LoggerFactory.getLogger(SQLiteStorage.class);

    public static final String METADATA_KEY = "metadata";

    public static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        OBJECT_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    static {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            LOG.error("Failed to load SQLite driver", e);
        }
    }

    private String dbFile;

    public SQLiteStorage(String dbFile) {
        this.dbFile = dbFile;
    }

    /**
     * Open poly storage connection
     * @return
     * @throws SQLException
     */
    public Connection openDb() throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:" + dbFile);
    }

    /**
     * Migrate storage records
     */
    public void migrateStorage() {
        Flyway flyway = new Flyway();
        flyway.setDataSource("jdbc:sqlite:" + dbFile, null, null);
        flyway.setOutOfOrder(true);
        flyway.setLocations("db/sqlitestorage");
        flyway.migrate();
    }


    @Override
    public <P extends Poly> P persist(P poly) {
        try(Connection connection = openDb()) {

            String rawJSON = OBJECT_MAPPER.writeValueAsString(poly);

            PreparedStatement dataStatement = connection.prepareStatement("SELECT * FROM data WHERE _id = ?;");
            dataStatement.setString(1, poly._id());
            ResultSet dataResultSet = dataStatement.executeQuery();


            if (!dataResultSet.next()) {
                PreparedStatement preparedStatement = connection.prepareStatement("INSERT OR REPLACE INTO data(_id, data) VALUES(?, ?);");
                preparedStatement.setString(1, poly._id());
                preparedStatement.setObject(2, rawJSON);
                preparedStatement.executeUpdate();
            } else {
                PreparedStatement preparedStatement = connection.prepareStatement("INSERT OR REPLACE INTO data(id, _id, data) VALUES(?, ?, ?);");
                preparedStatement.setObject(1, dataResultSet.getObject("id"));
                preparedStatement.setString(2, poly._id());
                preparedStatement.setObject(3, rawJSON);
                preparedStatement.executeUpdate();
            }
            return poly;
        } catch (Exception e) {
            LOG.error("Failed to import poly {}", poly, e);
            return poly;
        }
    }

    @Override
    public boolean remove(String id) {
        try(Connection connection = openDb()) {
            PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM data WHERE _id = ?");
            preparedStatement.setString(1, id);
            return preparedStatement.executeUpdate() != 0;
        } catch (Exception e) {
            LOG.error("Failed to remove poly {}", id, e);
            return false;
        }
    }

    @Override
    public <P extends Poly> P metadata() {
        try {
            Optional<Poly> metadataPoly = fetchMetadata(METADATA_KEY);
            if (!metadataPoly.isPresent()) {
                return null;
            }
            return (P) metadataPoly.get();
        } catch (SQLiteStorageException e) {
            LOG.warn("Failed to fetch metadata", e);
            return null;
        }
    }

    public void metadata(Poly metadata){
        try {
            persistMetadata(METADATA_KEY, metadata);
        } catch (SQLiteStorageException e) {
            LOG.warn("Failed to persist", e);
        }
    }

    @Override
    public <P extends Poly> P fetchById(String id) {
        try(Connection connection = openDb()) {
            PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM data WHERE _id = ?;");
            preparedStatement.setString(1, id);

            ResultSet resultSet = preparedStatement.executeQuery();
            if (!resultSet.next()) {
                return null;
            }

            String rawJSON = resultSet.getString("data");

            return (P) OBJECT_MAPPER.readValue(rawJSON, BasicPoly.class);
        } catch (Exception e) {
            LOG.warn("Failed to fetch poly by id {}", id, e);
            return null;
        }
    }

    @Override
    public Collection<? extends Poly> list() {
        List<Poly> list = new ArrayList<>();
        try(Connection connection = openDb()) {
            PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM data ");
            ResultSet resultSet = preparedStatement.executeQuery();
            while(resultSet.next()) {
                String rawJSON = resultSet.getString("data");
                list.add(OBJECT_MAPPER.readValue(rawJSON, BasicPoly.class));
            }
        } catch (Exception e) {
            LOG.warn("Failed to list polys", e);
        }

        return list;
    }

    @Override
    public long size() {
        try(Connection connection = openDb()) {
            PreparedStatement preparedStatement = connection.prepareStatement("SELECT COUNT(*) AS count FROM data ");
            ResultSet resultSet = preparedStatement.executeQuery();
            return resultSet.getLong("count");
        } catch (Exception e) {
            LOG.warn("Failed to fetch poly count ", e);
            return 0;
        }
    }


    /**
     * Persist custom metadata in storage
     * @param key
     * @param poly
     * @throws SQLiteStorageException
     */
    public void persistMetadata(String key, Poly poly) throws SQLiteStorageException {
        try(Connection connection = openDb()) {
            PreparedStatement preparedStatement = connection.prepareStatement("INSERT OR REPLACE INTO metadata(_id, data) VALUES(?, ?);");
            String rawJSON = OBJECT_MAPPER.writeValueAsString(poly);
            preparedStatement.setString(1, key);
            preparedStatement.setObject(2, rawJSON);

            preparedStatement.executeUpdate();
        } catch (Exception e) {
            throw new SQLiteStorageException(e);
        }
    }

    /**
     * Fetch metadata
     * @param key
     * @return
     * @throws SQLiteStorageException
     */
    public Optional<Poly> fetchMetadata(String key) throws SQLiteStorageException {
        try(Connection connection = openDb()) {
            PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM metadata WHERE _id = ?;");
            preparedStatement.setString(1, key);

            ResultSet resultSet = preparedStatement.executeQuery();
            if (!resultSet.next()) {
                return Optional.empty();
            }

            String rawJSON = resultSet.getString("data");

            return Optional.of(OBJECT_MAPPER.readValue(rawJSON, BasicPoly.class));
        } catch (Exception e) {
            throw new SQLiteStorageException(e);
        }
    }

}
