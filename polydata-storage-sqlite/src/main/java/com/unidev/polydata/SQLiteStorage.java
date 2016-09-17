package com.unidev.polydata;

/**
 * SQLite Polydata storage
 */
public class SQLiteStorage {

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



}
