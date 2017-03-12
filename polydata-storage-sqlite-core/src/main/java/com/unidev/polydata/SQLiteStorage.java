/**
 * Copyright (c) 2017 Denis O <denis.o@linux.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.unidev.polydata;

import com.unidev.polydata.domain.BasicPoly;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Optional;

import static com.unidev.polydata.SQLitePolyConstants.POLY_OBJECT_MAPPER;

/**
 * Named polydata storage,
 * Each poly storage will be dedicated table
 */
public class SQLiteStorage {

    private static Logger LOG = LoggerFactory.getLogger(SQLiteStorage.class);

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

    /**
     * Fetch support poly by id
     * @return
     */
    public Optional<BasicPoly> fetchRawPoly(Connection connection, String table, String id) {
        PreparedStatement preparedStatement;
        try {
            preparedStatement = connection.prepareStatement("SELECT * FROM " + table + " WHERE _id = ?");
            preparedStatement.setString(1, id);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                String rawJSON = resultSet.getString(SQLitePolyConstants.DATA_KEY);
                return Optional.of(POLY_OBJECT_MAPPER.readValue(rawJSON, BasicPoly.class));
            }
            return Optional.empty();
        } catch (Exception e) {
            LOG.warn("Failed to fetch support poly {} {} {}", table, id, dbFile, e);
            return Optional.empty();
        }
    }

//
//
//    @Override
//    public <P extends Poly> P persist(P poly) {
//        try(Connection connection = openDb()) {
//
//            String rawJSON = OBJECT_MAPPER.writeValueAsString(poly);
//
//            PreparedStatement dataStatement = connection.prepareStatement("SELECT * FROM data WHERE _id = ?;");
//            dataStatement.setString(1, poly._id());
//            ResultSet dataResultSet = dataStatement.executeQuery();
//
//
//            if (!dataResultSet.next()) {
//                PreparedStatement preparedStatement = connection.prepareStatement("INSERT OR REPLACE INTO data(_id, data) VALUES(?, ?);");
//                preparedStatement.setString(1, poly._id());
//                preparedStatement.setObject(2, rawJSON);
//                preparedStatement.executeUpdate();
//            } else {
//                PreparedStatement preparedStatement = connection.prepareStatement("INSERT OR REPLACE INTO data(id, _id, data) VALUES(?, ?, ?);");
//                preparedStatement.setObject(1, dataResultSet.getObject("id"));
//                preparedStatement.setString(2, poly._id());
//                preparedStatement.setObject(3, rawJSON);
//                preparedStatement.executeUpdate();
//            }
//            return poly;
//        } catch (Exception e) {
//            LOG.error("Failed to import poly {}", poly, e);
//            return poly;
//        }
//    }
//
//    @Override
//    public boolean remove(String id) {
//        try(Connection connection = openDb()) {
//            PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM data WHERE _id = ?");
//            preparedStatement.setString(1, id);
//            return preparedStatement.executeUpdate() != 0;
//        } catch (Exception e) {
//            LOG.error("Failed to remove poly {}", id, e);
//            return false;
//        }
//    }
//
//    @Override
//    public <P extends Poly> P metadata() {
//        try {
//            Optional<Poly> metadataPoly = fetchMetadata(METADATA_KEY);
//            if (!metadataPoly.isPresent()) {
//                return null;
//            }
//            return (P) metadataPoly.get();
//        } catch (SQLiteStorageException e) {
//            LOG.warn("Failed to fetch metadata", e);
//            return null;
//        }
//    }
//
//    public void metadata(Poly metadata){
//        try {
//            persistMetadata(METADATA_KEY, metadata);
//        } catch (SQLiteStorageException e) {
//            LOG.warn("Failed to persist", e);
//        }
//    }
//
//    @Override
//    public <P extends Poly> P fetchById(String id) {
//        try(Connection connection = openDb()) {
//            PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM data WHERE _id = ?;");
//            preparedStatement.setString(1, id);
//
//            ResultSet resultSet = preparedStatement.executeQuery();
//            if (!resultSet.next()) {
//                return null;
//            }
//
//            String rawJSON = resultSet.getString("data");
//
//            return (P) OBJECT_MAPPER.readValue(rawJSON, BasicPoly.class);
//        } catch (Exception e) {
//            LOG.warn("Failed to fetch poly by id {}", id, e);
//            return null;
//        }
//    }
//
//    @Override
//    public Collection<? extends Poly> list() {
//        List<Poly> list = new ArrayList<>();
//        try(Connection connection = openDb()) {
//            PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM data ");
//            ResultSet resultSet = preparedStatement.executeQuery();
//            while(resultSet.next()) {
//                String rawJSON = resultSet.getString("data");
//                list.add(OBJECT_MAPPER.readValue(rawJSON, BasicPoly.class));
//            }
//        } catch (Exception e) {
//            LOG.warn("Failed to list polys", e);
//        }
//
//        return list;
//    }
//
//    @Override
//    public long size() {
//        try(Connection connection = openDb()) {
//            PreparedStatement preparedStatement = connection.prepareStatement("SELECT COUNT(*) AS count FROM data ");
//            ResultSet resultSet = preparedStatement.executeQuery();
//            return resultSet.getLong("count");
//        } catch (Exception e) {
//            LOG.warn("Failed to fetch poly count ", e);
//            return 0;
//        }
//    }
//
//
//    /**
//     * Persist custom metadata in storage
//     * @param key
//     * @param poly
//     * @throws SQLiteStorageException
//     */
//    public void persistMetadata(String key, Poly poly) throws SQLiteStorageException {
//        try(Connection connection = openDb()) {
//            PreparedStatement preparedStatement = connection.prepareStatement("INSERT OR REPLACE INTO metadata(_id, data) VALUES(?, ?);");
//            String rawJSON = OBJECT_MAPPER.writeValueAsString(poly);
//            preparedStatement.setString(1, key);
//            preparedStatement.setObject(2, rawJSON);
//
//            preparedStatement.executeUpdate();
//        } catch (Exception e) {
//            throw new SQLiteStorageException(e);
//        }
//    }
//
//    /**
//     * Fetch metadata
//     * @param key
//     * @return
//     * @throws SQLiteStorageException
//     */
//    public Optional<Poly> fetchMetadata(String key) throws SQLiteStorageException {
//        try(Connection connection = openDb()) {
//            PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM metadata WHERE _id = ?;");
//            preparedStatement.setString(1, key);
//
//            ResultSet resultSet = preparedStatement.executeQuery();
//            if (!resultSet.next()) {
//                return Optional.empty();
//            }
//
//            String rawJSON = resultSet.getString("data");
//
//            return Optional.of(OBJECT_MAPPER.readValue(rawJSON, BasicPoly.class));
//        } catch (Exception e) {
//            throw new SQLiteStorageException(e);
//        }
//    }

}
