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

import com.unidev.polydata.storage.PolyStorage;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Named polydata storage,
 * Each poly storage will be dedicated table
 */
@Slf4j
public abstract class AbstractEmbeddedStorage implements PolyStorage {

    protected String dbFile;

    public AbstractEmbeddedStorage(String dbFile) {
        this.dbFile = dbFile;
    }

    public String getDbFile() {
        return dbFile;
    }

    protected Connection connection;

    public Connection fetchConnection() {
        if (connection == null) {
            synchronized (this) {
                if (connection == null) {
                    connection = openDb();
                }
            }
        }
        return connection;
    }

    /**
     * Open poly storage connection
     *
     * @return
     * @throws SQLException
     */
    public abstract Connection openDb();

    /**
     * Close DB
     */
    public void closeDb() throws EmbeddedStorageException {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.error("Failed to close connection for {}", dbFile, e);
                throw new EmbeddedStorageException(e);
            }
        }
    }

    /**
     * Migrate storage records
     */
    public abstract void migrateStorage();


}
