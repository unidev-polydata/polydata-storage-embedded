package com.unidev.polydata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.unidev.polydata.domain.BasicPoly;
import com.unidev.polydata.domain.Poly;

import java.io.IOException;
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

    public SQLiteStorage save(String polyName, Poly poly) throws SQLiteStorageException {
        createDB(polyName);

        if (fetch(polyName, poly._id()).isPresent()) {
            return this;
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
            PreparedStatement preparedStatement = connection.prepareStatement(
                            "INSERT INTO " + polyName +
                                    "(" + String.join(",", keys) +")" +
                            " VALUES ( " + String.join(",", qmarks) + " )");
            for(int id = 0;id<values.size();id++) {
                preparedStatement.setObject(id +1, values.get(id));
            }
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new SQLiteStorageException(e);
        }
        return this;
    }

    public Optional<Poly> fetch(String polyName, String id) {
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


    protected void createDB(String name) {
        try (Connection connection = openDb()) {
            Statement statement = connection.createStatement();
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS "+name+" (_id TEXT PRIMARY KEY, json JSON)");
        } catch (SQLException e) {
            e.printStackTrace();
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
