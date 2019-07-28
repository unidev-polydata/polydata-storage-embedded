/**
 * Copyright (c) 2017 Denis O <denis.o@linux.com>
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.unidev.polydata;

import com.unidev.polydata.domain.BasicPoly;
import com.unidev.polydata.domain.Poly;
import com.unidev.polydata.domain.PolyList;
import com.unidev.polydata.domain.PolyQuery;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.flywaydb.core.Flyway;

import java.sql.*;
import java.util.*;

import static com.unidev.polydata.EmbeddedPolyConstants.*;

/**
 * Named polydata storage,
 * Each poly storage will be dedicated table
 */
@Slf4j
public class SQLiteStorage extends AbstractEmbeddedStorage {

    static {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            log.error("Failed to load SQLite driver", e);
        }
    }

    public SQLiteStorage(String dbFile) {
        super(dbFile);
    }

    /**
     * Open poly storage connection
     */
    public Connection openDb() {
        try {
            return DriverManager.getConnection("jdbc:sqlite:" + dbFile);
        } catch (SQLException e) {
            log.warn("Failed to open db {}", dbFile, e);
            throw new EmbeddedStorageException(e);
        }
    }

    /**
     * Migrate storage records
     */
    public void migrateStorage() {
        Flyway flyway = new Flyway();
        flyway.setDataSource("jdbc:sqlite:" + dbFile, null, null);
        flyway.setOutOfOrder(true);
        flyway.setLocations("db/polystorage");
        flyway.migrate();
    }

    @Override
    public <P extends Poly> Optional<P> metadata(String container) {
        return (Optional<P>) fetchRawPoly(fetchConnection(), TYPE_METADATA, container, container);
    }

    @Override
    public <P extends Poly> P persistMetadata(String container, P metadata) {
        return persistRawPoly(fetchConnection(), TYPE_METADATA, container, metadata._id(), metadata);
    }

    @Override
    public <P extends Poly> Optional<P> fetchById(String container, String id) {
        return (Optional<P>) fetchRawPoly(fetchConnection(), TYPE_DATA, container, id);
    }

    @Override
    public <P extends Poly> P persist(String container, P poly) {
        boolean newPoly = !existRawPoly(fetchConnection(), TYPE_DATA, container, poly._id());
        P persistedPoly = persistRawPoly(fetchConnection(), TYPE_DATA, container, poly._id(), poly);
        List<String> tags = poly.fetch(TAGS_KEY);
        if (!CollectionUtils.isEmpty(tags)) {
            for (String tag : tags) {
                persistIndex(container, Collections.singletonMap(TAGS_KEY, tag), BasicPoly.newPoly(poly._id()));
            }
            if (newPoly) {
                Optional<BasicPoly> tagsMapOptional = fetchRawPoly(fetchConnection(), TYPE_POLYMAP, container, TAGS_KEY);
                BasicPoly tagsMap = tagsMapOptional.orElseGet(() -> BasicPoly.newPoly(TAGS_KEY));
                for (String tag : tags) {
                    int count = tagsMap.fetch(tag, 0);
                    count++;
                    tagsMap.put(tag, count);
                }
                persistRawPoly(fetchConnection(), TYPE_POLYMAP, container, TAGS_KEY, tagsMap);

            }
        }
        return persistedPoly;
    }

    /**
     * Fetch tag map
     * @param container
     * @return
     */
    public BasicPoly fetchTags(String container) {
        return fetchRawPoly(fetchConnection(), TYPE_POLYMAP, container, TAGS_KEY).orElseGet(BasicPoly::new);
    }

    @Override
    public <P extends Poly> P persistIndex(String container, Map<String, Object> keys, P poly) {
        List<String> list = new ArrayList<>(keys.keySet());
        Collections.sort(list);
        StringBuilder indexId = new StringBuilder();
        for (String key : list) {
            indexId.append(key).append(":").append(keys.get(key));
        }
        persistRawPoly(fetchConnection(), TYPE_POLY_INDEX, container, indexId.toString(), poly);
        return null;
    }

    @Override
    public <P extends PolyList> P query(String container, PolyQuery polyQuery) {
        SQLitePolyQuery query = (SQLitePolyQuery) polyQuery;

        return null;
    }

    @Override
    public <P extends PolyList> P queryIndex(String container, PolyQuery polyQuery) {
        SQLitePolyQuery query = (SQLitePolyQuery) polyQuery;


        return null;
    }

    @Override
    public boolean removePoly(String container, String id) {
        return removeRawPoly(fetchConnection(), TYPE_DATA, container, id);
    }

    /**
     * Fetch support poly by id
     *
     * @return
     */
    public <P extends Poly> Optional<BasicPoly> fetchRawPoly(Connection connection, String type, String container, String id) {
        PreparedStatement preparedStatement;
        try {
            preparedStatement = connection.prepareStatement("SELECT * FROM " + DATA + " WHERE container=? AND _type = ? AND  _id = ?");
            preparedStatement.setString(1, container);
            preparedStatement.setString(2, type);
            preparedStatement.setString(3, id);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                String rawJSON = resultSet.getString(EmbeddedPolyConstants.DATA_KEY);
                return Optional.of(POLY_OBJECT_MAPPER.readValue(rawJSON, BasicPoly.class));
            }
            return Optional.empty();
        } catch (Exception e) {
            log.warn("Failed to fetch support poly {} {} {}", type, id, dbFile, e);
            return Optional.empty();
        }
    }

    /**
     * Remove raw poly from db
     */
    public boolean removeRawPoly(Connection connection, String type, String container, String id) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM " + DATA + " WHERE container=? AND _type = ? AND _id = ? ");
            preparedStatement.setString(1, container);
            preparedStatement.setString(2, type);
            preparedStatement.setString(3, id);
            return preparedStatement.executeUpdate() != 0;
        } catch (Exception e) {
            log.error("Failed to remove poly {} {} {}", type, id, dbFile, e);
            return false;
        }
    }

    public <P extends Poly> P persistRawPoly(Connection connection, String type, String container, String id, P poly) {
        try {
            String rawJSON = POLY_OBJECT_MAPPER.writeValueAsString(poly);

            PreparedStatement dataStatement = connection.prepareStatement("SELECT * FROM " + DATA + " WHERE container=? AND _type = ? AND _id = ?;");
            dataStatement.setString(1, container);
            dataStatement.setString(2, type);
            dataStatement.setString(3, id);
            ResultSet dataResultSet = dataStatement.executeQuery();
            if (!dataResultSet.next()) { // insert
                PreparedStatement preparedStatement = connection.prepareStatement("INSERT OR REPLACE INTO " + DATA + " (container, _type, _id, data) VALUES(?, ?, ?, ?);");
                preparedStatement.setString(1, container);
                preparedStatement.setString(2, type);
                preparedStatement.setString(3, id);
                preparedStatement.setObject(4, rawJSON);
                preparedStatement.executeUpdate();
            } else { // update
                PreparedStatement preparedStatement = connection.prepareStatement("INSERT OR REPLACE INTO " + DATA + "(id, container, _type, _id, data, update_date) VALUES(?, ?, ?, ?, ?, ?);");
                preparedStatement.setObject(1, dataResultSet.getObject("id"));
                preparedStatement.setString(2, container);
                preparedStatement.setString(3, type);
                preparedStatement.setString(4, id);
                preparedStatement.setObject(5, rawJSON);
                preparedStatement.setObject(6, new java.util.Date());
                preparedStatement.executeUpdate();
            }
        } catch (Exception e) {
            log.error("Failed to persist poly {}", poly, e);
            throw new EmbeddedStorageException(e);
        }
        return poly;
    }

    public boolean existRawPoly(Connection connection, String type, String container, String id) {
        try {
            PreparedStatement dataStatement = connection.prepareStatement("SELECT * FROM " + DATA + " WHERE container=? AND _type = ? AND _id = ?;");
            dataStatement.setString(1, container);
            dataStatement.setString(2, type);
            dataStatement.setString(3, id);
            ResultSet dataResultSet = dataStatement.executeQuery();
            return dataResultSet.next();
        } catch (Exception e) {
            log.error("Failed to query poly {}", id, e);
            throw new EmbeddedStorageException(e);
        }
    }


//
//    /**
//     * Fetch poly by id
//     * @param id
//     * @return
//     */
//    public Optional<BasicPoly> fetchPoly(Connection connection, String id) {
//        return fetchRawPoly(connection, EmbeddedPolyConstants.DATA_KEY, id);
//    }
//
//    /**
//     * Batch fetch polys by id
//     * @param polyIds
//     * @return
//     */
//    public Map<String, Optional<BasicPoly>> fetchPolyMap(Connection connection, Collection<String> polyIds) {
//        Map<String, Optional<BasicPoly>> result = new HashMap<>();
//        for(String id : polyIds) {
//            result.put(id, fetchPoly(connection, id));
//        }
//        return result;
//    }
//
//    /**
//     * Batch fetch polys by id
//     * @param polyIds
//     * @return
//     */
//    public Collection<Optional<BasicPoly>> fetchPolys(Connection connection, Collection<String> polyIds) {
//        List<Optional<BasicPoly>> polys = new ArrayList<>();
//        for(String id : polyIds) {
//            polys.add(fetchPoly(connection, id));
//        }
//        return polys;
//    }
//
//    /**
//     * Persist storage poly
//     * @param connection
//     * @param poly
//     * @return
//     */
//    public BasicPoly persistPoly(Connection connection, BasicPoly poly) {
//        try {
//            String rawJSON = POLY_OBJECT_MAPPER.writeValueAsString(poly);
//
//            String rawTags = null;
//            Collection tags = poly.fetch(EmbeddedPolyConstants.TAGS_KEY);
//            if (tags != null) {
//                rawTags = POLY_OBJECT_MAPPER.writeValueAsString(tags);
//            }
//
//            PreparedStatement dataStatement = connection.prepareStatement("SELECT * FROM " + EmbeddedPolyConstants.DATA_POLY + " WHERE _id = ?;");
//            dataStatement.setString(1, poly._id());
//            ResultSet dataResultSet = dataStatement.executeQuery();
//            java.util.Date date = new java.util.Date();
//            if (!dataResultSet.next()) { // insert
//                PreparedStatement preparedStatement = connection.prepareStatement("INSERT OR REPLACE INTO " + EmbeddedPolyConstants.DATA_POLY + "(_id, tags, data, create_date, update_date) VALUES(?, ?, ?, ?, ?);");
//                preparedStatement.setString(1, poly._id());
//                preparedStatement.setString(2, rawTags);
//                preparedStatement.setObject(3, rawJSON);
//
//                preparedStatement.setObject(4, date);
//                preparedStatement.setObject(5, date);
//                preparedStatement.executeUpdate();
//            } else { // update
//                PreparedStatement preparedStatement = connection.prepareStatement("INSERT OR REPLACE INTO " + EmbeddedPolyConstants.DATA_POLY + "(id, _id, tags, data, create_date, update_date) VALUES(?, ?, ?, ?, ?,?);");
//                preparedStatement.setObject(1, dataResultSet.getObject("id"));
//                preparedStatement.setString(2, poly._id());
//                preparedStatement.setString(3, rawTags);
//                preparedStatement.setObject(4, rawJSON);
//                preparedStatement.setObject(5, dataResultSet.getObject("create_date"));
//                preparedStatement.setObject(6, date);
//                preparedStatement.executeUpdate();
//            }
//        }catch (Exception e) {
//            LOG.error("Failed to persist poly {}", poly, e);
//            throw new EmbeddedStorageException(e);
//        }
//        return poly;
//    }
//
//    // count polys
//
//    /**
//     * Count poly records
//     * @param connection
//     * @param polyQuery
//     * @return
//     */
//    public long fetchPolyCount(Connection connection, EmbeddedPolyQuery polyQuery) {
//        try {
//            PreparedStatement preparedStatement;
//            StringBuilder query = new StringBuilder("SELECT COUNT(*) AS count FROM " + EmbeddedPolyConstants.DATA_POLY + " WHERE 1=1 ");
//            preparedStatement = buildPolyQuery(polyQuery, false, connection, query);
//            return preparedStatement.executeQuery().getLong("count");
//        }catch (Exception e) {
//            LOG.warn("Failed to fetch polys {}", dbFile, e);
//            throw new EmbeddedStorageException(e);
//        }
//    }
//
//    /**
//     * Query poly records
//     * @param connection
//     * @param polyQuery
//     * @return
//     */
//    public List<BasicPoly> listPoly(Connection connection, EmbeddedPolyQuery polyQuery) {
//        try {
//            PreparedStatement preparedStatement;
//            StringBuilder query = new StringBuilder("SELECT * FROM " + EmbeddedPolyConstants.DATA_POLY + " WHERE 1=1 ");
//            preparedStatement = buildPolyQuery(polyQuery, true, connection, query);
//            return evaluateStatementToPolyList(preparedStatement);
//        }catch (Exception e) {
//            LOG.warn("Failed to fetch polys {}", dbFile, e);
//            throw new EmbeddedStorageException(e);
//        }
//    }
//
//    private PreparedStatement buildPolyQuery(EmbeddedPolyQuery sqlitePolyQuery, boolean includePagination, Connection connection, StringBuilder query) throws SQLException {
//        Integer id = 1;
//        Map<Integer, Object> params = new HashMap<>();
//        PreparedStatement preparedStatement;
//
//        if (sqlitePolyQuery.getTag() != null) {
//            query.append(" AND " + EmbeddedPolyConstants.TAGS_KEY + " LIKE ?");
//            params.put(id++, "%" + sqlitePolyQuery.getTag() + "%");
//        }
//
//
//
//        if (includePagination) {
//            if (sqlitePolyQuery.getItemPerPage() != null) {
//
//                if (Boolean.TRUE.equals(sqlitePolyQuery.getRandomOrder())) {
//                    query.append(" ORDER BY RANDOM() ");
//                } else {
//                    query.append(" ORDER BY update_date DESC ");
//                }
//
//                query.append("  LIMIT ? OFFSET ?");
//                params.put(id++, sqlitePolyQuery.getItemPerPage());
//                params.put(id++, sqlitePolyQuery.getItemPerPage() * (sqlitePolyQuery.getPage()));
//            }
//        }
//
//        preparedStatement = connection.prepareStatement(query.toString());
//        for (Map.Entry<Integer, Object> entry : params.entrySet()) {
//            preparedStatement.setObject(entry.getKey(), entry.getValue());
//        }
//        return preparedStatement;
//    }
//    /**
//     * Remove poly by ID
//     * @param connection
//     * @param polyId
//     * @return
//     */
//    public boolean removePoly(Connection connection, String polyId) {
//        return removeRawPoly(connection, EmbeddedPolyConstants.DATA_POLY, polyId);
//    }
//
//    /**
//     * Persist tag record
//     * @param connection
//     * @param tagPoly
//     * @return
//     */
//    public BasicPoly persistTag(Connection connection, BasicPoly tagPoly) {
//        try {
//
//            Optional<BasicPoly> tagById = fetchTagPoly(connection, tagPoly._id());
//
//            if (!tagById.isPresent()) {
//                tagPoly.put(EmbeddedPolyConstants.COUNT_KEY, 1);
//                String rawJSON = POLY_OBJECT_MAPPER.writeValueAsString(tagPoly);
//
//                PreparedStatement preparedStatement = connection.prepareStatement("INSERT OR REPLACE INTO " + EmbeddedPolyConstants.TAGS_POLY + "(_id, count, data) VALUES(?, ?, ?);");
//                preparedStatement.setString(1, tagPoly._id());
//                preparedStatement.setLong(2, 1L);
//                preparedStatement.setObject(3, rawJSON);
//                preparedStatement.executeUpdate();
//            } else {
//                tagPoly.put(EmbeddedPolyConstants.COUNT_KEY, ((int)tagById.get().fetch(EmbeddedPolyConstants.COUNT_KEY) + 1));
//                String rawJSON = POLY_OBJECT_MAPPER.writeValueAsString(tagPoly);
//                PreparedStatement preparedStatement = connection.prepareStatement("UPDATE " + EmbeddedPolyConstants.TAGS_POLY + " SET count = count + 1, data =? WHERE _id = ?;");
//                preparedStatement.setString(1, rawJSON);
//                preparedStatement.setString(2, tagPoly._id());
//                preparedStatement.executeUpdate();
//            }
//        } catch (Exception e) {
//            LOG.error("Failed to persist tag poly {}", tagPoly, e);
//            throw new EmbeddedStorageException(e);
//        }
//        return fetchRawPoly(connection, TAGS_POLY, tagPoly._id()).orElseThrow(EmbeddedStorageException::new);
//    }
//
//    @Override
//    public BasicPoly persistTag(Connection connection, String tagStorage, BasicPoly tagPoly) {
//        throw new UnsupportedOperationException("Not implemented");
//    }
//
//    /**
//     * Fetching tag by id
//     * @param connection
//     * @return
//     */
//    public List<BasicPoly> fetchTags(Connection connection) {
//        try {
//            PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM " + EmbeddedPolyConstants.TAGS_POLY + " ORDER BY count DESC");
//            return evaluateStatementToPolyList(preparedStatement);
//        } catch (SQLException e) {
//            LOG.warn("Failed to fetch tags", e);
//            return Collections.EMPTY_LIST;
//        }
//    }
//
//    @Override
//    public List<BasicPoly> fetchTags(Connection connection, String tagStorage) {
//        throw new UnsupportedOperationException("Not implemented");
//    }
//
//
//    /**
//     * Fetch tag polys
//     * @param connection
//     * @param id
//     * @return
//     */
//    public Optional<BasicPoly> fetchTagPoly(Connection connection, String id) {
//        return fetchRawPoly(connection, EmbeddedPolyConstants.TAGS_POLY, id);
//    }
//
//    @Override
//    public Optional<BasicPoly> fetchTagPoly(Connection connection, String tagStorage, String id) {
//        throw new UnsupportedOperationException("Not implemented");
//    }
//
//    // count tags
//
//    /**
//     * Count tag records in tag poly
//     * @param connection
//     * @return
//     */
//    public long fetchTagCount(Connection connection) {
//        return fetchPolyCount(connection, EmbeddedPolyConstants.TAGS_POLY);
//    }
//
//    @Override
//    public long fetchTagCount(Connection connection, String tagStorage) {
//        throw new UnsupportedOperationException("Not implemented");
//    }
//
//    // fetch tag index list
//
//    /**
//     * Fetch tag index for specific tag
//     * @param connection
//     * @param tagIndex
//     * @return
//     */
//    public List<BasicPoly> fetchTagIndex(Connection connection, String tagIndex) {
//        try {
//
//            PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM " + tagIndex + " ");
//            return evaluateStatementToPolyList(preparedStatement);
//        } catch (SQLException e) {
//            LOG.warn("Failed to fetch tags", e);
//            return Collections.EMPTY_LIST;
//        }
//    }
//
//    /**
//     * Fetch tag index poly from index by documentId
//     * @return
//     */
//    public Optional<BasicPoly> fetchTagIndexPoly(Connection connection, String tagIndex, String documentId) {
//        return fetchRawPoly(connection, tagIndex + ".tag_index", documentId);
//    }
//
//    // count tags
//
//    /**
//     * Count tag records in tag poly
//     * @return
//     */
//    public long fetchTagIndexCount(Connection connection, String tagIndex) {
//        return fetchPolyCount(connection, tagIndex + ".tag_index");
//    }
//
//    /**
//     * Count available polys from support table
//     * @param table
//     * @return
//     */
//    public Long fetchPolyCount(Connection connection, String table) {
//        PreparedStatement preparedStatement;
//        try {
//            preparedStatement = connection.prepareStatement("SELECT COUNT(*) AS count FROM " + table + "");
//            return preparedStatement.executeQuery().getLong("count");
//        } catch (SQLException e) {
//            LOG.warn("Failed to fetch poly count from {}", table, e);
//            throw new EmbeddedStorageException(e);
//        }
//    }
//
//    private List<BasicPoly> evaluateStatementToPolyList(PreparedStatement preparedStatement) {
//
//        List<BasicPoly> polyList = new ArrayList<>();
//        try {
//            ResultSet resultSet = preparedStatement.executeQuery();
//
//            while (resultSet.next()) {
//                String rawJSON = resultSet.getString(EmbeddedPolyConstants.DATA_KEY);
//                BasicPoly polyRecord = POLY_OBJECT_MAPPER.readValue(rawJSON, BasicPoly.class);
//                polyList.add(polyRecord);
//            }
//            return polyList;
//        } catch (Exception e) {
//            LOG.warn("Failed to evaluate statement {}", dbFile, e);
//            throw new EmbeddedStorageException(e);
//        }
//    }
//

//    // persist tag index
//
//    /**
//     * Persist tag index record
//     * @param connection
//     * @param tagIndex
//     * @param documentId
//     * @param data
//     * @return
//     */
//    public BasicPoly persistIndexTag(Connection connection, String tagIndex, String documentId, BasicPoly data) {
//        try {
//            String rawJSON = POLY_OBJECT_MAPPER.writeValueAsString(data);
//
//            PreparedStatement dataStatement = connection.prepareStatement("SELECT * FROM " + tagIndex + ".tag_index WHERE _id = ?;");
//            dataStatement.setString(1, documentId);
//            ResultSet dataResultSet = dataStatement.executeQuery();
//
//            if (!dataResultSet.next()) {
//                PreparedStatement preparedStatement = connection.prepareStatement("INSERT OR REPLACE INTO " + tagIndex + ".tag_index(_id, data) VALUES(?, ?);");
//                preparedStatement.setString(1, documentId);
//                preparedStatement.setObject(2, rawJSON);
//                preparedStatement.executeUpdate();
//            } else {
//                PreparedStatement preparedStatement = connection.prepareStatement("UPDATE " + tagIndex + ".tag_index SET data=? WHERE _id=?;");
//                preparedStatement.setString(1, rawJSON);
//                preparedStatement.setString(2, documentId);
//                preparedStatement.executeUpdate();
//            }
//        } catch (Exception e) {
//            LOG.error("Failed to persist tag index poly {}", data, e);
//            throw new EmbeddedStorageException(e);
//        }
//        return fetchRawPoly(connection, tagIndex + ".tag_index", documentId).orElseThrow(EmbeddedStorageException::new);
//
//    }
//
//    public void attachTagIndexDb(Connection connection, String tagIndex) throws SQLException {
//        File tagIndexFile = new File(new File(dbFile).getParentFile(), tagIndex + ".db");
//
//        Flyway flyway = new Flyway();
//        flyway.setDataSource("jdbc:sqlite:" + tagIndexFile.getAbsolutePath(), null, null);
//        flyway.setOutOfOrder(true);
//        flyway.setLocations("db/polyindex");
//        flyway.migrate();
//
//        connection.prepareStatement("ATTACH DATABASE \"" + tagIndexFile.getAbsolutePath() + "\" AS " + tagIndex).execute();
//    }

}
