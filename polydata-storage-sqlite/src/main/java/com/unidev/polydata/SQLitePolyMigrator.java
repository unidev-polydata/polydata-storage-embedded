package com.unidev.polydata;

import java.sql.Connection;

/**
 * SQLite polydata creator interface
 */
public interface SQLitePolyMigrator {

    /**
     * Check if can handle poly creation
     * @param poly
     * @return
     */
    boolean canHandle(String poly);

    /**
     * Handle polydata migration
     * @param poly
     * @param connection
     * @throws SQLiteStorageException
     */
    void handle(String poly, Connection connection) throws SQLiteStorageException;

}
