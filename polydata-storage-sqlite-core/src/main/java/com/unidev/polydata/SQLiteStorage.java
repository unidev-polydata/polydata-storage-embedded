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
import java.text.MessageFormat;
import java.util.*;

import static com.unidev.polydata.SQLitePolyConstants.POLY_OBJECT_MAPPER;
import static com.unidev.polydata.SQLitePolyConstants.TAGS_POLY;

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
     * Fetch poly by id
     * @param id
     * @return
     */
    public Optional<BasicPoly> fetchPoly(Connection connection, String id) {
        return fetchRawPoly(connection, SQLitePolyConstants.DATA_KEY, id);
    }

    /**
     * Batch fetch polys by id
     * @param polyIds
     * @return
     */
    public Map<String, Optional<BasicPoly>> fetchPolyMap(Connection connection, Collection<String> polyIds) {
        Map<String, Optional<BasicPoly>> result = new HashMap<>();
        for(String id : polyIds) {
            result.put(id, fetchPoly(connection, id));
        }
        return result;
    }

    /**
     * Batch fetch polys by id
     * @param polyIds
     * @return
     */
    public Collection<Optional<BasicPoly>> fetchPolys(Connection connection, Collection<String> polyIds) {
        List<Optional<BasicPoly>> polys = new ArrayList<>();
        for(String id : polyIds) {
            polys.add(fetchPoly(connection, id));
        }
        return polys;
    }

    /**
     * Persist storage poly
     * @param connection
     * @param poly
     * @return
     */
    public BasicPoly persistPoly(Connection connection, BasicPoly poly) {
        try {
            String rawJSON = POLY_OBJECT_MAPPER.writeValueAsString(poly);

            PreparedStatement dataStatement = connection.prepareStatement("SELECT * FROM " + SQLitePolyConstants.DATA_POLY + " WHERE _id = ?;");
            dataStatement.setString(1, poly._id());
            ResultSet dataResultSet = dataStatement.executeQuery();

            if (!dataResultSet.next()) { // insert
                PreparedStatement preparedStatement = connection.prepareStatement("INSERT OR REPLACE INTO " + SQLitePolyConstants.DATA_POLY + "(_id, tags, data) VALUES(?, ?, ?);");
                preparedStatement.setString(1, poly._id());
                Set<String> tags = poly.fetch(SQLitePolyConstants.TAGS_KEY);
                if (tags == null) {
                    preparedStatement.setString(2, null);
                } else {
                    preparedStatement.setString(2, String.join(",", tags)); //TODO: replace with complex object
                }
                preparedStatement.setObject(3, rawJSON);
                preparedStatement.executeUpdate();
            } else { // update
                PreparedStatement preparedStatement = connection.prepareStatement("INSERT OR REPLACE INTO " + SQLitePolyConstants.DATA_POLY + "(id, _id, tags, data) VALUES(?, ?, ?, ?);");
                preparedStatement.setObject(1, dataResultSet.getObject("id"));
                preparedStatement.setString(2, poly._id());
                Set<String> tags = poly.fetch(SQLitePolyConstants.TAGS_KEY);
                if (tags == null) {
                    preparedStatement.setString(3, null);
                } else {
                    preparedStatement.setString(3, String.join(",", tags));
                }
                preparedStatement.setObject(4, rawJSON);
                preparedStatement.executeUpdate();
            }
        }catch (Exception e) {
            LOG.error("Failed to persist poly {}", poly, e);
            throw new SQLiteStorageException(e);
        }
        return poly;
    }

    // count polys

    // query polys

    /**
     * Remove poly by ID
     * @param connection
     * @param polyId
     * @return
     */
    public boolean removePoly(Connection connection, String polyId) {
        return removeRawPoly(connection, SQLitePolyConstants.DATA_POLY, polyId);
    }

    /**
     * Persist tag record
     * @param connection
     * @param tagPoly
     * @return
     */
    public BasicPoly persistTag(Connection connection, BasicPoly tagPoly) {
        try {
            String rawJSON = POLY_OBJECT_MAPPER.writeValueAsString(tagPoly);

            PreparedStatement dataStatement = connection.prepareStatement("SELECT * FROM " + SQLitePolyConstants.TAGS_POLY + " WHERE _id = ?;");
            dataStatement.setString(1, tagPoly._id());
            ResultSet dataResultSet = dataStatement.executeQuery();

            if (!dataResultSet.next()) {
                PreparedStatement preparedStatement = connection.prepareStatement("INSERT OR REPLACE INTO " + SQLitePolyConstants.TAGS_POLY + "(_id, count, data) VALUES(?, ?, ?);");
                preparedStatement.setString(1, tagPoly._id());
                preparedStatement.setLong(2, 1L);
                preparedStatement.setObject(3, rawJSON);
                preparedStatement.executeUpdate();
            } else {
                PreparedStatement preparedStatement = connection.prepareStatement("UPDATE " + SQLitePolyConstants.TAGS_POLY + " SET _id = ?, count = count + 1, data =? WHERE id=?;");
                preparedStatement.setString(1, tagPoly._id());
                preparedStatement.setObject(2, rawJSON);
                preparedStatement.executeUpdate();
            }
        } catch (Exception e) {
            LOG.error("Failed to persist tag poly {}", tagPoly, e);
            throw new SQLiteStorageException(e);
        }
        return fetchRawPoly(connection, TAGS_POLY, tagPoly._id()).orElseThrow(SQLiteStorageException::new);
    }

    /**
     * Fetching tag by id
     * @param connection
     * @return
     */
    public List<BasicPoly> fetchTags(Connection connection) {

        try {
            PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM " + SQLitePolyConstants.TAGS_POLY + " ORDER BY count DESC");
            return evaluateStatementToPolyList(preparedStatement);
        } catch (SQLException e) {
            LOG.warn("Failed to fetch tags", e);
            return Collections.EMPTY_LIST;
        }
    }


    /**
     * Fetch tag polys
     * @param connection
     * @param id
     * @return
     */
    public Optional<BasicPoly> fetchTagPoly(Connection connection, String id) {
        return fetchRawPoly(connection, SQLitePolyConstants.TAGS_POLY, id);
    }

    // count tags

    /**
     * Count tag records in tag poly
     * @param connection
     * @return
     */
    public Optional<Long> fetchTagCount(Connection connection) {
        return fetchPolyCount(connection, SQLitePolyConstants.TAGS_POLY);
    }

    // persist tag index

    /**
     * Persist tag index record
     * @param connection
     * @param tagIndex
     * @param documentId
     * @param data
     * @return
     */
    public BasicPoly persistIndexTag(Connection connection, String tagIndex, String documentId, BasicPoly data) {
        try {

            PreparedStatement checkTableStatement = connection.prepareStatement("SELECT COUNT(name) AS count FROM sqlite_master WHERE name=?");
            checkTableStatement.setString(1, tagIndex);
            if (checkTableStatement.executeQuery().getLong("count") == 0) {
                createIndexTagStorage(connection, tagIndex);
            }

            String rawJSON = POLY_OBJECT_MAPPER.writeValueAsString(data);

            PreparedStatement dataStatement = connection.prepareStatement("SELECT * FROM " + tagIndex + " WHERE _id = ?;");
            dataStatement.setString(1, documentId);
            ResultSet dataResultSet = dataStatement.executeQuery();

            if (!dataResultSet.next()) {
                PreparedStatement preparedStatement = connection.prepareStatement("INSERT OR REPLACE INTO " + tagIndex + "(_id,tag, data) VALUES(?,?);");
                preparedStatement.setString(1, documentId);
                preparedStatement.setString(2, data._id());
                preparedStatement.setObject(3, rawJSON);
                preparedStatement.executeUpdate();
            } else {
                PreparedStatement preparedStatement = connection.prepareStatement("UPDATE " + tagIndex + " SET _id = ?, tag = ?, data =? WHERE id=?;");
                preparedStatement.setString(1, documentId);
                preparedStatement.setString(2, data._id());
                preparedStatement.setObject(3, rawJSON);
                preparedStatement.executeUpdate();
            }
        } catch (Exception e) {
            LOG.error("Failed to persist tag index poly {}", data, e);
            throw new SQLiteStorageException(e);
        }
        return fetchRawPoly(connection, tagIndex, documentId).orElseThrow(SQLiteStorageException::new);

    }

    // fetch tag index list

    /**
     * Fetch tag index for specific tag
     * @param connection
     * @param tagIndex
     * @return
     */
    public List<BasicPoly> fetchTagIndex(Connection connection, String tagIndex) {

        try {
            PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM " + tagIndex + " ");
            return evaluateStatementToPolyList(preparedStatement);
        } catch (SQLException e) {
            LOG.warn("Failed to fetch tags", e);
            return Collections.EMPTY_LIST;
        }
    }

    /**
     * Fetch tag polys
     * @param connection
     * @param id
     * @return
     */
    public Optional<BasicPoly> fetchTagIndexPoly(Connection connection, String tagIndex, String id) {
        return fetchRawPoly(connection, tagIndex, id);
    }

    // count tags

    /**
     * Count tag records in tag poly
     * @return
     */
    public Optional<Long> fetchTagIndexCount(Connection connection, String tagIndex) {
        return fetchPolyCount(connection, tagIndex);
    }

    // fetch tag index item

    private final static String TAG_INDEX_TABLE =
            "CREATE TABLE {0} (\n" +
                    "  id INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
                    "  _id TEXT,\n" +
                    "  tag TEXT,\n" +
                    "  data JSON\n" +
                    ");\n" +
                    "\n" +
                    "CREATE INDEX {0}_id_idx ON {0} (_id);" +
                    "CREATE INDEX {0}_tag_idx ON {0} (tag);" +
                    "";

    private void createIndexTagStorage(Connection connection, String tagIndex) throws SQLException {
        String rawSQL = MessageFormat.format(TAG_INDEX_TABLE, tagIndex);
        try(Statement statement = connection.createStatement()) {
            statement.execute(rawSQL);
        }
    }

    /**
     * Count available polys from support table
     * @param table
     * @return
     */
    public Optional<Long> fetchPolyCount(Connection connection, String table) {
        PreparedStatement preparedStatement;
        try {
            preparedStatement = connection.prepareStatement("SELECT COUNT(*) AS count FROM " + table + "");
            return Optional.of(preparedStatement.executeQuery().getLong("count"));
        } catch (SQLException e) {
            LOG.warn("Failed to fetch poly count from {}", table, e);
            return Optional.empty();
        }
    }

    private List<BasicPoly> evaluateStatementToPolyList(PreparedStatement preparedStatement) {

        List<BasicPoly> polyList = new ArrayList<>();
        try {
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                String rawJSON = resultSet.getString(SQLitePolyConstants.DATA_KEY);
                BasicPoly polyRecord = POLY_OBJECT_MAPPER.readValue(rawJSON, BasicPoly.class);
                try {
                    polyRecord.put("count", resultSet.getObject("count"));
                }catch (SQLException e) {}
                polyList.add(polyRecord);
            }
            return polyList;
        } catch (Exception e) {
            LOG.warn("Failed to evaluate statement {}", dbFile, e);
            throw new SQLiteStorageException(e);
        }
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

    /**
     * Remove raw poly from db
     */
    public boolean removeRawPoly(Connection connection, String table, String id) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM " + table + " WHERE _id = ?");
            preparedStatement.setString(1, id);
            return preparedStatement.executeUpdate() != 0;
        } catch (Exception e) {
            LOG.error("Failed to remove poly {} {} {}", table, id, dbFile, e);
            return false;
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
