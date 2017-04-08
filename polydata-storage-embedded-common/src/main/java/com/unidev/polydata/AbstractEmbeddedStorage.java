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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Named polydata storage,
 * Each poly storage will be dedicated table
 */
public abstract class AbstractEmbeddedStorage {


    protected String dbFile;

    public AbstractEmbeddedStorage(String dbFile) {
        this.dbFile = dbFile;
    }

    /**
     * Open poly storage connection
     * @return
     * @throws SQLException
     */
    public abstract Connection openDb();

    /**
     * Migrate storage records
     */
    public abstract void migrateStorage();


    /**
     * Fetch poly by id
     * @param id
     * @return
     */
    public abstract Optional<BasicPoly> fetchPoly(Connection connection, String id);

    /**
     * Batch fetch polys by id
     * @param polyIds
     * @return
     */
    public abstract Map<String, Optional<BasicPoly>> fetchPolyMap(Connection connection, Collection<String> polyIds);

    /**
     * Batch fetch polys by id
     * @param polyIds
     * @return
     */
    public abstract Collection<Optional<BasicPoly>> fetchPolys(Connection connection, Collection<String> polyIds);

    /**
     * Persist storage poly
     * @param connection
     * @param poly
     * @return
     */
    public abstract BasicPoly persistPoly(Connection connection, BasicPoly poly);

    // count polys

    /**
     * Count poly records
     * @param connection
     * @param polyQuery
     * @return
     */
    public abstract long fetchPolyCount(Connection connection, EmbeddedPolyQuery polyQuery);

    /**
     * Query poly records
     * @param connection
     * @param polyQuery
     * @return
     */
    public abstract List<BasicPoly> listPoly(Connection connection, EmbeddedPolyQuery polyQuery);

    /**
     * Remove poly by ID
     * @param connection
     * @param polyId
     * @return
     */
    public abstract boolean removePoly(Connection connection, String polyId);

    /**
     * Persist tag record
     * @param connection
     * @param tagPoly
     * @return
     */
    public abstract BasicPoly persistTag(Connection connection, BasicPoly tagPoly);

    /**
     * Fetching tag by id
     * @param connection
     * @return
     */
    public abstract List<BasicPoly> fetchTags(Connection connection);

    /**
     * Fetch tag polys
     * @param connection
     * @param id
     * @return
     */
    public abstract Optional<BasicPoly> fetchTagPoly(Connection connection, String id);

    // count tags

    /**
     * Count tag records in tag poly
     * @param connection
     * @return
     */
    public abstract long fetchTagCount(Connection connection);

    // persist tag index

    /**
     * Persist tag index record
     * @param connection
     * @param tagIndex
     * @param documentId
     * @param data
     * @return
     */
    public abstract BasicPoly persistIndexTag(Connection connection, String tagIndex, String documentId, BasicPoly data);

    // fetch tag index list

    /**
     * Fetch tag index for specific tag
     * @param connection
     * @param tagIndex
     * @return
     */
    public abstract List<BasicPoly> fetchTagIndex(Connection connection, String tagIndex);

    /**
     * Fetch tag index poly from index by documentId
     * @return
     */
    public abstract Optional<BasicPoly> fetchTagIndexPoly(Connection connection, String tagIndex, String documentId);

    /**
     * Fetch tag index by tag
     */
    public abstract Optional<BasicPoly> fetchTagIndexPolyByTag(Connection connection, String tagIndex, String tag);

    // count tags

    /**
     * Count tag records in tag poly
     * @return
     */
    public abstract long fetchTagIndexCount(Connection connection, String tagIndex);

    /**
     * Count available polys from support table
     * @param table
     * @return
     */
    public abstract Long fetchPolyCount(Connection connection, String table);

    /**
     * Fetch support poly by id
     * @return
     */
    public abstract Optional<BasicPoly> fetchRawPoly(Connection connection, String table, String id);

    /**
     * Remove raw poly from db
     */
    public abstract boolean removeRawPoly(Connection connection, String table, String id);

}
