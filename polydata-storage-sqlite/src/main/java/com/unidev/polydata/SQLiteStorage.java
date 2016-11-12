package com.unidev.polydata;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.unidev.polydata.domain.BasicPoly;
import com.unidev.polydata.domain.Poly;
import com.unidev.polydata.storage.ChangablePolyStorage;
import org.flywaydb.core.Flyway;

import java.sql.*;
import java.util.Collection;
import java.util.Optional;

/**
 * Named polydata storage,
 * Each poly storage will be dedicated table
 */
public class SQLiteStorage implements ChangablePolyStorage {

    public static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        OBJECT_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    static {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
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
        return null;
    }

    @Override
    public boolean remove(String id) {
        return false;
    }

    @Override
    public <P extends Poly> P metadata() {
        return null;
    }

    @Override
    public <P extends Poly> P fetchById(String id) {
        return null;
    }

    @Override
    public Collection<? extends Poly> list() {
        return null;
    }

    @Override
    public long size() {
        return 0;
    }


    /**
     * Persist custom metadata in storage
     * @param key
     * @param poly
     * @throws SQLiteStorageException
     */
    public void persistMetadata(String key, Poly poly) throws SQLiteStorageException {
        try(Connection connection = openDb()) {
            PreparedStatement preparedStatement = connection.prepareStatement("INSERT OR REPLACE INTO metadata VALUES(?, ?);");
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

    //    private Collection<SQLitePolyMigrator> polyMigrators;

//
//    /**
//     * Persist poly into storage
//     * @param polyName
//     * @param poly
//     * @return
//     * @throws SQLiteStorageException
//     */
//    public SQLiteStorage save(String polyName, Poly poly) throws SQLiteStorageException {
//        createDB(polyName);
//
//        boolean update = false;
//
//        if (fetch(polyName, poly._id()).isPresent()) {
//            update = true;
//        }
//
//        try(Connection connection = openDb()) {
//            List<String> keys = new ArrayList<>();
//            List<Object> values = new ArrayList<>();
//            List<String> qmarks = new ArrayList<>();
//            poly.forEach( (k,v) -> {
//                keys.add(k);
//                values.add(v);
//                qmarks.add("?");
//            });
//            PreparedStatement preparedStatement;
//            if (update) {
//                preparedStatement = null;
//
//                List<String> setValues = new ArrayList<>();
//                keys.forEach( key -> { setValues.add(key + " = ?"); });
//
//                String updateQuery = "UPDATE " + polyName + " SET "
//                        + String.join(",", setValues)
//                        + " WHERE _id = ?";
//
//                preparedStatement = connection.prepareStatement(updateQuery);
//                for(int id = 0;id<values.size();id++) {
//                    preparedStatement.setObject(id +1, values.get(id));
//                }
//                preparedStatement.setObject(values.size() + 1 , poly._id());
//            } else {
//
//                String insertQuery = "INSERT INTO " + polyName +
//                        "(" + String.join(",", keys) +")" +
//                        " VALUES ( " + String.join(",", qmarks) + " )";
//
//                preparedStatement = connection.prepareStatement(insertQuery);
//                for(int id = 0;id<values.size();id++) {
//                    preparedStatement.setObject(id +1, values.get(id));
//                }
//            }
//
//
//            preparedStatement.execute();
//        } catch (SQLException e) {
//            throw new SQLiteStorageException(e);
//        }
//        return this;
//    }
//
//    /**
//     * Fetch poly by id
//     * @param polyName
//     * @param id
//     * @return
//     */
//    public Optional<BasicPoly> fetch(String polyName, String id) {
//        try (Connection connection = openDb()){
//            ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM " + polyName + " WHERE _id = '" + id + "' ;");
//            if (!resultSet.next()) {
//                return Optional.empty();
//            }
//            ResultSetMetaData metaData = resultSet.getMetaData();
//            BasicPoly result = new BasicPoly();
//
//            for(int column = 1;column <= metaData.getColumnCount(); column++) {
//                String columnName = metaData.getColumnName(column);
//                result.put(columnName, resultSet.getObject(columnName));
//            }
//            return Optional.of(result);
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//
//        return Optional.empty();
//    }
//
//    /**
//     * Remove poly from storage
//     * @param polyName
//     * @param id
//     * @throws SQLiteStorageException
//     */
//    public void remove(String polyName, String id) throws SQLiteStorageException {
//        try (Connection connection = openDb()) {
//            PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM " + polyName + " WHERE _id = ?");
//            preparedStatement.setObject(1, id);
//            preparedStatement.execute();
//        } catch (SQLException e) {
//            throw new SQLiteStorageException(e);
//        }
//    }
//
//    /**
//     * Evaluate statement and try to map results as poly list
//     * @param preparedStatement
//     * @return
//     * @throws SQLiteStorageException
//     */
//    public List<BasicPoly> evaluateStatement(PreparedStatement preparedStatement) throws SQLiteStorageException {
//
//        try {
//            ResultSet resultSet = preparedStatement.executeQuery();
//            List<BasicPoly> result = new ArrayList<>();
//            ResultSetMetaData metaData = resultSet.getMetaData();
//            List<String> keys = new ArrayList<>();
//            for(int column = 1;column <= metaData.getColumnCount(); column++) {
//                String columnName = metaData.getColumnName(column);
//                keys.add(columnName);
//            }
//
//            while(resultSet.next()) {
//                BasicPoly basicPoly = new BasicPoly();
//                for(String key : keys) {
//                    basicPoly.put(key, resultSet.getObject(key));
//                }
//                result.add(basicPoly);
//            }
//
//            return result;
//        } catch (SQLException e) {
//            throw new SQLiteStorageException(e);
//        }
//    }
//
//
//    protected void createDB(String name) throws SQLiteStorageException {
//
//        Optional<SQLitePolyMigrator> migrator = polyMigrators.stream().filter(m -> m.canHandle(name)).findFirst();
//        migrator.orElseThrow(SQLiteStorageException::new);
//
//        try (Connection connection = openDb()) {
//            migrator.get().handle(name, connection);
//        } catch (SQLException e) {
//            throw new SQLiteStorageException(e);
//        }
//    }


}
