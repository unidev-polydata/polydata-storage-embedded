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

import com.unidev.polydata.domain.*;
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
        return persistRawPoly(fetchConnection(), TYPE_METADATA, container, metadata._id(), true, metadata);
    }

    @Override
    public <P extends Poly> Optional<P> fetchById(String container, String id) {
        return (Optional<P>) fetchRawPoly(fetchConnection(), TYPE_DATA, container, id);
    }

    @Override
    public <P extends Poly> P persist(String container, P poly) {
        boolean newPoly = !existRawPoly(fetchConnection(), TYPE_DATA, container, poly._id());
        P persistedPoly = persistRawPoly(fetchConnection(), TYPE_DATA, container, poly._id(), true, poly);
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
                persistRawPoly(fetchConnection(), TYPE_POLYMAP, container, TAGS_KEY, true, tagsMap);

            }
        }
        return persistedPoly;
    }

    /**
     * Fetch tag map
     *
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
        return persistRawPoly(fetchConnection(), TYPE_POLY_INDEX, container, indexId.toString(), false, poly);
    }

    @Override
    public <P extends PolyList> P query(String container, PolyQuery polyQuery) {
        EmbeddedPolyQuery query = (EmbeddedPolyQuery) polyQuery;
        return (P) queryPoly(container, TYPE_DATA, query);
    }

    @Override
    public <P extends PolyList> P queryIndex(String container, PolyQuery polyQuery) {
        EmbeddedPolyQuery query = (EmbeddedPolyQuery) polyQuery;
        BasicPolyList basicPolyList = new BasicPolyList();
        BasicPolyList polyIndexList = queryPoly(container, TYPE_POLY_INDEX, query, "_id", "tags:" + query.getTag());

        for(BasicPoly poly : polyIndexList.list()) {
            Optional<Poly> optionalPoly = fetchById(container, poly._id());
            if (!optionalPoly.isPresent()) {
                continue;
            }
            basicPolyList.add(optionalPoly.get());
        }

        return (P) basicPolyList;
    }

    public long fetchPolyCount(String container) {
        return fetchRawPolyCount(fetchConnection(), TYPE_DATA, container);

    }

    private BasicPolyList queryPoly(String container, String type, EmbeddedPolyQuery query, String... parameters) {
        try {
            StringBuilder sqlQuery = new StringBuilder("SELECT * FROM " + DATA + " WHERE container=? AND _type=? ");
            Integer id = 1;
            Map<Integer, Object> params = new HashMap<>();
            params.put(id++, container);
            params.put(id++, type);

            if (parameters.length !=0 ) {
                for(int i = 0;i<parameters.length;i++) {
                    sqlQuery.append(" AND " + parameters[i] + "=?");
                    i++;
                    params.put(id++, parameters[i]);
                }
            }

            if (query.getItemPerPage() != null) {
                if (Boolean.TRUE.equals(query.getRandomOrder())) {
                    sqlQuery.append(" ORDER BY RANDOM() ");
                } else {
                    sqlQuery.append(" ORDER BY update_date DESC ");
                }

                sqlQuery.append("  LIMIT ? OFFSET ?");
                params.put(id++, query.getItemPerPage());
                params.put(id++, query.getItemPerPage() * (query.getPage()));
            }
            PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery.toString());
            for (Map.Entry<Integer, Object> entry : params.entrySet()) {
                preparedStatement.setObject(entry.getKey(), entry.getValue());
            }
            return evaluateStatementToPolyList(preparedStatement);
        } catch (Exception e) {
            log.warn("Failed to fetch polys {}", dbFile, e);
            throw new EmbeddedStorageException(e);
        }
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

    public <P extends Poly> P persistRawPoly(Connection connection, String type, String container, String id, boolean duplicateCheck, P poly) {
        try {
            String rawJSON = POLY_OBJECT_MAPPER.writeValueAsString(poly);

            ResultSet dataResultSet = null;

            boolean insert = true;

            if (duplicateCheck) {
                PreparedStatement dataStatement = connection.prepareStatement("SELECT * FROM " + DATA + " WHERE container=? AND _type = ? AND _id = ?;");
                dataStatement.setString(1, container);
                dataStatement.setString(2, type);
                dataStatement.setString(3, id);
                dataResultSet = dataStatement.executeQuery();
                if (dataResultSet.next()) { // insert
                    insert = false;
                }
            }
            if (insert) { // insert
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

    public long fetchRawPolyCount(Connection connection, String type, String container) {
        PreparedStatement preparedStatement;
        try {
            preparedStatement = connection.prepareStatement("SELECT COUNT(*) AS count FROM " + DATA + " WHERE container = ? AND _type = ?");
            preparedStatement.setString(1, container);
            preparedStatement.setString(2, type);
            return preparedStatement.executeQuery().getLong("count");
        } catch (SQLException e) {
            log.warn("Failed to fetch poly count from {}", type, e);
            throw new EmbeddedStorageException(e);
        }
    }

    private BasicPolyList evaluateStatementToPolyList(PreparedStatement preparedStatement) {
        BasicPolyList polyList = BasicPolyList.newList();
        try {
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String rawJSON = resultSet.getString(EmbeddedPolyConstants.DATA_KEY);
                BasicPoly polyRecord = POLY_OBJECT_MAPPER.readValue(rawJSON, BasicPoly.class);
                polyList.add(polyRecord);
            }
            return polyList;
        } catch (Exception e) {
            log.warn("Failed to evaluate statement {}", dbFile, e);
            throw new EmbeddedStorageException(e);
        }
    }

}
