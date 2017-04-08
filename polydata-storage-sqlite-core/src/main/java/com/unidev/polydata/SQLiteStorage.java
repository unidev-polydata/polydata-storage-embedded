/**
 * Copyright (c) 2017 Denis O <denis.o@linux.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.unidev.polydata;

import com.unidev.polydata.domain.BasicPoly;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.MessageFormat;

import static com.unidev.polydata.EmbeddedPolyConstants.POLY_OBJECT_MAPPER;

/**
 * Named polydata storage,
 * Each poly storage will be dedicated table
 */
public class SQLiteStorage extends AbstractEmbeddedStorage {

    private static Logger LOG = LoggerFactory.getLogger(SQLiteStorage.class);

    static {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            LOG.error("Failed to load SQLite driver", e);
        }
    }

    public SQLiteStorage(String dbFile) {
        super(dbFile);
    }

    /**
     * Open poly storage connection
     * @return
     * @throws SQLException
     */
    public Connection openDb() {
        try {
            return DriverManager.getConnection("jdbc:sqlite:" + dbFile);
        } catch (SQLException e) {
            LOG.warn("Failed to open db {}", dbFile, e);
            throw new EmbeddedStorageException(e);
        }
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

    // persist tag index

    /**
     * Persist tag index record
     * @param connection
     * @param tagIndex
     * @param documentId
     * @param data
     * @return
     */
    public BasicPoly persistIndexTag(Connection connection, String tagIndex, String documentId, BasicPoly data) {
        try {

            PreparedStatement checkTableStatement = connection.prepareStatement("SELECT COUNT(name) AS count FROM sqlite_master WHERE name=?");
            checkTableStatement.setString(1, tagIndex);
            if (checkTableStatement.executeQuery().getLong("count") == 0) {
                createIndexTagStorage(connection, tagIndex);
            }

            String rawJSON = POLY_OBJECT_MAPPER.writeValueAsString(data);

            PreparedStatement dataStatement = connection.prepareStatement("SELECT * FROM " + tagIndex + " WHERE _id = ?;");
            dataStatement.setString(1, documentId);
            ResultSet dataResultSet = dataStatement.executeQuery();

            if (!dataResultSet.next()) {
                PreparedStatement preparedStatement = connection.prepareStatement("INSERT OR REPLACE INTO " + tagIndex + "(_id,tag, data) VALUES(?,?, ?);");
                preparedStatement.setString(1, documentId);
                preparedStatement.setString(2, data._id());
                preparedStatement.setObject(3, rawJSON);
                preparedStatement.executeUpdate();
            } else {
                PreparedStatement preparedStatement = connection.prepareStatement("UPDATE " + tagIndex + " SET _id = ?, tag = ?, data =? WHERE id=?;");
                preparedStatement.setString(1, documentId);
                preparedStatement.setString(2, data._id());
                preparedStatement.setObject(3, rawJSON);
                preparedStatement.setObject(4, dataResultSet.getObject("id"));
                preparedStatement.executeUpdate();
            }
        } catch (Exception e) {
            LOG.error("Failed to persist tag index poly {}", data, e);
            throw new EmbeddedStorageException(e);
        }
        return fetchRawPoly(connection, tagIndex, documentId).orElseThrow(EmbeddedStorageException::new);

    }

    // count tags

    // fetch tag index item

    private final static String TAG_INDEX_TABLE =
            "CREATE TABLE {0} (\n" +
                    "  id INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
                    "  _id TEXT,\n" +
                    "  tag TEXT,\n" +
                    "  data JSON,\n" +
                    "create_date datetime DEFAULT CURRENT_TIMESTAMP," +
                    "update_date datetime DEFAULT CURRENT_TIMESTAMP" +
                    ");\n" +
                    "\n" +
                    "CREATE INDEX {0}_id_idx ON {0} (_id);" +
                    "CREATE INDEX {0}_tag_idx ON {0} (tag);" +
                    "CREATE INDEX {0}_update_date_idx ON {0} (update_date);" +
                    "";
    private final static String TAG_INDEX_TABLE_UPDATE =
            "ALTER TABLE {0} ADD  create_date datetime ;\n" +
                    "UPDATE {0} SET create_date = datetime();\n" +
                    "ALTER TABLE {0} ADD  update_date datetime ;\n" +
                    "UPDATE {0} SET update_date = datetime();\n" +
                    "CREATE INDEX {0}_update_date_idx ON {0} (update_date);";

    private void createIndexTagStorage(Connection connection, String tagIndex) throws SQLException {

        PreparedStatement checkTableStatement = connection.prepareStatement("SELECT COUNT(name) AS count FROM sqlite_master WHERE name=?");
        checkTableStatement.setString(1, tagIndex);

        if (checkTableStatement.executeQuery().getLong("count") == 0) { // create table
            String rawSQL = MessageFormat.format(TAG_INDEX_TABLE, tagIndex);
            try(Statement statement = connection.createStatement()) {
                statement.execute(rawSQL);
            }
        }  else { // upgrade
            String rawSQL = MessageFormat.format(TAG_INDEX_TABLE_UPDATE, tagIndex);
            try(Statement statement = connection.createStatement()) {
                statement.execute(rawSQL);
            }
        }

    }

}
