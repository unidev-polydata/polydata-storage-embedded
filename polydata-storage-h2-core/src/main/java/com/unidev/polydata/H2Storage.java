package com.unidev.polydata;


import com.unidev.polydata.domain.BasicPoly;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

import static com.unidev.polydata.EmbeddedPolyConstants.POLY_OBJECT_MAPPER;

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
        return fetchRawPoly(connection, EmbeddedPolyConstants.DATA_KEY, id);
    }

    @Override
    public Map<String, Optional<BasicPoly>> fetchPolyMap(Connection connection, Collection<String> polyIds) {
        Map<String, Optional<BasicPoly>> result = new HashMap<>();
        for(String id : polyIds) {
            result.put(id, fetchPoly(connection, id));
        }
        return result;
    }

    @Override
    public Collection<Optional<BasicPoly>> fetchPolys(Connection connection, Collection<String> polyIds) {
        List<Optional<BasicPoly>> polys = new ArrayList<>();
        for(String id : polyIds) {
            polys.add(fetchPoly(connection, id));
        }
        return polys;
    }

    @Override
    public BasicPoly persistPoly(Connection connection, BasicPoly poly) {
        try {
            String rawJSON = POLY_OBJECT_MAPPER.writeValueAsString(poly);

            String rawTags = null;
            Collection tags = poly.fetch(EmbeddedPolyConstants.TAGS_KEY);
            if (tags != null) {
                rawTags = POLY_OBJECT_MAPPER.writeValueAsString(tags);
            }

            PreparedStatement dataStatement = connection.prepareStatement("SELECT * FROM " + EmbeddedPolyConstants.DATA_POLY + " WHERE _id = ?;");
            dataStatement.setString(1, poly._id());
            ResultSet dataResultSet = dataStatement.executeQuery();
            java.util.Date date = new java.util.Date();
            if (!dataResultSet.next()) { // insert
                PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO " + EmbeddedPolyConstants.DATA_POLY + "(_id, tags, data, create_date, update_date) VALUES(?, ?, ?, ?, ?);");
                preparedStatement.setString(1, poly._id());
                preparedStatement.setString(2, rawTags);
                preparedStatement.setObject(3, rawJSON);

                preparedStatement.setObject(4, date);
                preparedStatement.setObject(5, date);
                preparedStatement.executeUpdate();
            } else { // update
                PreparedStatement preparedStatement = connection.prepareStatement("UPDATE " + EmbeddedPolyConstants.DATA_POLY + " SET tags = ?, data = ? WHERE _id = ?  ;");
                preparedStatement.setObject(1, rawTags);
                preparedStatement.setString(2, rawJSON);
                preparedStatement.setString(3, poly._id());
                preparedStatement.executeUpdate();
            }
        }catch (Exception e) {
            LOG.error("Failed to persist poly {}", poly, e);
            throw new EmbeddedStorageException(e);
        }
        return poly;
    }

    @Override
    public long fetchPolyCount(Connection connection, EmbeddedPolyQuery polyQuery) {
        try {
            PreparedStatement preparedStatement;
            StringBuilder query = new StringBuilder("SELECT COUNT(*) AS count FROM " + EmbeddedPolyConstants.DATA_POLY + " WHERE 1=1 ");
            preparedStatement = buildPolyQuery(polyQuery, false, connection, query);
            return preparedStatement.executeQuery().getLong("count");
        }catch (Exception e) {
            LOG.warn("Failed to fetch polys {}", dbFile, e);
            throw new EmbeddedStorageException(e);
        }
    }

    @Override
    public List<BasicPoly> listPoly(Connection connection, EmbeddedPolyQuery polyQuery) {
        try {
            PreparedStatement preparedStatement;
            StringBuilder query = new StringBuilder("SELECT * FROM " + EmbeddedPolyConstants.DATA_POLY + " WHERE 1=1 ");
            preparedStatement = buildPolyQuery(polyQuery, true, connection, query);
            return evaluateStatementToPolyList(preparedStatement);
        }catch (Exception e) {
            LOG.warn("Failed to fetch polys {}", dbFile, e);
            throw new EmbeddedStorageException(e);
        }
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
        PreparedStatement preparedStatement;
        try {
            preparedStatement = connection.prepareStatement("SELECT * FROM " + table + " WHERE _id = ?");
            preparedStatement.setString(1, id);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                String rawJSON = resultSet.getString(EmbeddedPolyConstants.DATA_KEY);
                return Optional.of(POLY_OBJECT_MAPPER.readValue(rawJSON, BasicPoly.class));
            }
            return Optional.empty();
        } catch (Exception e) {
            LOG.warn("Failed to fetch support poly {} {} {}", table, id, dbFile, e);
            return Optional.empty();
        }
    }

    @Override
    public boolean removeRawPoly(Connection connection, String table, String id) {
        return false;
    }

    private PreparedStatement buildPolyQuery(EmbeddedPolyQuery sqlitePolyQuery, boolean includePagination, Connection connection, StringBuilder query) throws SQLException {
        Integer id = 1;
        Map<Integer, Object> params = new HashMap<>();
        PreparedStatement preparedStatement;

        if (sqlitePolyQuery.getTag() != null) {
            query.append(" AND " + EmbeddedPolyConstants.TAGS_KEY + " LIKE ?");
            params.put(id++, "%" + sqlitePolyQuery.getTag() + "%");
        }



        if (includePagination) {
            if (sqlitePolyQuery.getItemPerPage() != null) {

                if (Boolean.TRUE.equals(sqlitePolyQuery.getRandomOrder())) {
                    query.append(" ORDER BY RANDOM() ");
                } else {
                    query.append(" ORDER BY update_date DESC ");
                }

                query.append("  LIMIT ? OFFSET ?");
                params.put(id++, sqlitePolyQuery.getItemPerPage());
                params.put(id++, sqlitePolyQuery.getItemPerPage() * (sqlitePolyQuery.getPage()));
            }
        }

        preparedStatement = connection.prepareStatement(query.toString());
        for (Map.Entry<Integer, Object> entry : params.entrySet()) {
            preparedStatement.setObject(entry.getKey(), entry.getValue());
        }
        return preparedStatement;
    }

    private List<BasicPoly> evaluateStatementToPolyList(PreparedStatement preparedStatement) {

        List<BasicPoly> polyList = new ArrayList<>();
        try {
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                String rawJSON = resultSet.getString(EmbeddedPolyConstants.DATA_KEY);
                BasicPoly polyRecord = POLY_OBJECT_MAPPER.readValue(rawJSON, BasicPoly.class);
                polyList.add(polyRecord);
            }
            return polyList;
        } catch (Exception e) {
            LOG.warn("Failed to evaluate statement {}", dbFile, e);
            throw new EmbeddedStorageException(e);
        }
    }
}
