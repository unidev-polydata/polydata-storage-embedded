package com.unidev.polydata;

import com.unidev.polydata.domain.BasicPoly;
import com.unidev.polydata.domain.Poly;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Named polydata storage,
 * Each poly storage will be dedicated table
 */
public class SQLiteStorage {

    static {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    private Collection<SQLitePolyMigrator> polyMigrators;
    private String dbFile;

    public SQLiteStorage(String dbFile) {
        this.dbFile = dbFile;
    }

    /**
     * Persist poly into storage
     * @param polyName
     * @param poly
     * @return
     * @throws SQLiteStorageException
     */
    public SQLiteStorage save(String polyName, Poly poly) throws SQLiteStorageException {
        createDB(polyName);

        boolean update = false;

        if (fetch(polyName, poly._id()).isPresent()) {
            update = true;
        }

        try(Connection connection = openDb()) {
            List<String> keys = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            List<String> qmarks = new ArrayList<>();
            poly.forEach( (k,v) -> {
                keys.add(k);
                values.add(v);
                qmarks.add("?");
            });
            PreparedStatement preparedStatement;
            if (update) {
                preparedStatement = null;

                List<String> setValues = new ArrayList<>();
                keys.forEach( key -> { setValues.add(key + " = ?"); });

                String updateQuery = "UPDATE " + polyName + " SET "
                        + String.join(",", setValues)
                        + " WHERE _id = ?";

                preparedStatement = connection.prepareStatement(updateQuery);
                for(int id = 0;id<values.size();id++) {
                    preparedStatement.setObject(id +1, values.get(id));
                }
                preparedStatement.setObject(values.size() + 1 , poly._id());
            } else {

                String insertQuery = "INSERT INTO " + polyName +
                        "(" + String.join(",", keys) +")" +
                        " VALUES ( " + String.join(",", qmarks) + " )";

                preparedStatement = connection.prepareStatement(insertQuery);
                for(int id = 0;id<values.size();id++) {
                    preparedStatement.setObject(id +1, values.get(id));
                }
            }


            preparedStatement.execute();
        } catch (SQLException e) {
            throw new SQLiteStorageException(e);
        }
        return this;
    }

    /**
     * Fetch poly by id
     * @param polyName
     * @param id
     * @return
     */
    public Optional<BasicPoly> fetch(String polyName, String id) {
        try (Connection connection = openDb()){
            ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM " + polyName + " WHERE _id = '" + id + "' ;");
            if (!resultSet.next()) {
                return Optional.empty();
            }
            ResultSetMetaData metaData = resultSet.getMetaData();
            BasicPoly result = new BasicPoly();

            for(int column = 1;column <= metaData.getColumnCount(); column++) {
                String columnName = metaData.getColumnName(column);
                result.put(columnName, resultSet.getObject(columnName));
            }
            return Optional.of(result);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Optional.empty();
    }

    public void remove(String polyName, String id) throws SQLiteStorageException {
        try (Connection connection = openDb()) {
            PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM " + polyName + " WHERE _id = ?");
            preparedStatement.setObject(1, id);
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new SQLiteStorageException(e);
        }
    }

    public List<BasicPoly> evaluateStatement(PreparedStatement preparedStatement) throws SQLiteStorageException {

        try {
            ResultSet resultSet = preparedStatement.executeQuery();
            List<BasicPoly> result = new ArrayList<>();
            ResultSetMetaData metaData = resultSet.getMetaData();
            List<String> keys = new ArrayList<>();
            for(int column = 1;column <= metaData.getColumnCount(); column++) {
                String columnName = metaData.getColumnName(column);
                keys.add(columnName);
            }

            while(resultSet.next()) {
                BasicPoly basicPoly = new BasicPoly();
                for(String key : keys) {
                    basicPoly.put(key, resultSet.getObject(key));
                }
                result.add(basicPoly);
            }

            return result;
        } catch (SQLException e) {
            throw new SQLiteStorageException(e);
        }
    }


    protected void createDB(String name) throws SQLiteStorageException {

        Optional<SQLitePolyMigrator> migrator = polyMigrators.stream().filter(m -> m.canHandle(name)).findFirst();
        migrator.orElseThrow(SQLiteStorageException::new);

        try (Connection connection = openDb()) {
            migrator.get().handle(name, connection);
        } catch (SQLException e) {
            throw new SQLiteStorageException(e);
        }
    }

    public Connection openDb() throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:" + dbFile);
    }

    public Collection<SQLitePolyMigrator> getPolyMigrators() {
        return polyMigrators;
    }

    public void setPolyMigrators(Collection<SQLitePolyMigrator> polyMigrators) {
        this.polyMigrators = polyMigrators;
    }
}
