package com.unidev.polydata;

/**
 * SQLite storage exception
 */
public class SQLiteStorageException extends Exception {
    public SQLiteStorageException() {
        super();
    }

    public SQLiteStorageException(String message) {
        super(message);
    }

    public SQLiteStorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public SQLiteStorageException(Throwable cause) {
        super(cause);
    }
}
