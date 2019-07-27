package com.unidev.polydata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Test SQLite operations
 */
public class SQLiteTest {

    @Test
    public void testJson() throws ClassNotFoundException, SQLException, JsonProcessingException {
        Class.forName("org.sqlite.JDBC");

        Connection connection = DriverManager.getConnection("jdbc:sqlite:sample.db");
        Statement statement = connection.createStatement();
        statement.setQueryTimeout(30);  // set timeout to 30 sec.

        ObjectMapper objectMapper = new ObjectMapper();

        statement.executeUpdate("drop table if exists poly");
        statement.executeUpdate("create table poly (id TEXT, json JSON)");

        Map<String, String> object = new HashMap<>();
        object.put("key1", "qwe");
        object.put("key2", "123");
        String rawJson = objectMapper.writeValueAsString(object);

        statement.executeUpdate("insert into poly values('potato', '"+rawJson+"')");

        object = new HashMap<>();
        object.put("key1", "xyz");
        object.put("key2", "123");
        rawJson = objectMapper.writeValueAsString(object);

        statement.executeUpdate("insert into poly values('tomato', '"+rawJson+"' )");
        ResultSet rs = statement.executeQuery("select * from poly");
        //rs.getMetaData().getColumn
        while(rs.next())
        {
            // read the result set
            System.out.println("id = " + rs.getObject("id"));
            System.out.println("json = " + rs.getObject("json"));
        }

        System.out.println("***Json query***");
        rs = statement.executeQuery("select json_extract(poly.json, '$.key1') as data from poly ;");
        while(rs.next())
        {

            System.out.println("data = " + rs.getObject("data")  + " " + rs.getObject("data").getClass());
        }

    }
}
