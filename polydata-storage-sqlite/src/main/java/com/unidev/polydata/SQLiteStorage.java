package com.unidev.polydata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.unidev.polydata.domain.Poly;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Named polydata storage,
 * Each poly storage will be dedicated table
 */
public class SQLiteStorage {

    public static ObjectMapper DB_OBJECT_MAPPER = new ObjectMapper() {{
        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }};

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

    public SQLiteStorage save(String polyName, Poly poly) throws SQLiteStorageException {
        createDB(polyName);
        try(Connection connection = openDb()) {
            String json = DB_OBJECT_MAPPER.writeValueAsString(poly);
            Statement statement = connection.createStatement();
            statement.executeUpdate("INSERT INTO "+polyName+" VALUES ('"+poly._id()+"', '"+json+"')");
        } catch (JsonProcessingException | SQLException e) {
            throw new SQLiteStorageException(e);
        }
        return this;
    }

    protected void createDB(String name) {
        try (Connection connection = openDb()) {
            Statement statement = connection.createStatement();
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS "+name+" (id TEXT, json JSON)");
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public Connection openDb() throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:" + dbFile);
    }
}
