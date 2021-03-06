package com.unidev.polydata;

import com.unidev.platform.common.exception.UnidevRuntimeException;
import com.unidev.polydata.domain.BasicPoly;
import com.unidev.polydata.domain.bucket.BasicPolyBucket;
import com.unidev.polydata.model.ListResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Service for managing poly records
 */
@Service
public class StorageService {

    private static Logger LOG = LoggerFactory.getLogger(SQLiteStorage.class);


    public static final String DB_FILE = "polydata.db";
    public static final String JSON_FILE = "polydata.json";

    @Value("${storage.root}")
    private String storageRoot;

    public boolean existStorageRoot(String storage) {
        return fetchRootFile(storage).exists();
    }

    public boolean existDBStorage(String storage) {
        return fetchFile(storage, DB_FILE).exists();
    }

    public boolean existPolyBucket(String storage) {
        return fetchFile(storage, JSON_FILE).exists();
    }

    public SQLiteStorage sqLiteStorage(String storage) {
        return new SQLiteStorage( fetchFile(storage, DB_FILE).getAbsolutePath() );
    }

    public BasicPolyBucket polyBucket(String storage) {
        File file = fetchFile(storage, JSON_FILE);
        try (FileReader fileReader = new FileReader(file)) {
            return EmbeddedPolyConstants.POLY_OBJECT_MAPPER.readValue(fileReader, BasicPolyBucket.class);
        } catch (Exception e) {
            throw new UnidevRuntimeException(e);
        }
    }

    public ListResponse queryPoly(String storage, SQLitePolyQuery query) {
        SQLiteStorage sqLiteStorage = sqLiteStorage(storage);
        try (Connection connection = sqLiteStorage.openDb()) {

            ListResponse listResponse = new ListResponse();
            List<BasicPoly> polyList = sqLiteStorage.listPoly(connection, query);
            long count = sqLiteStorage.fetchPolyCount(connection, query);
            listResponse.setData(polyList);
            listResponse.setCount(count);
            return listResponse;
        } catch (SQLException e) {
            LOG.warn("Failed to query polys {} {}", storage, query, e);
            throw new EmbeddedStorageException(e);
        }
    }

    public List<BasicPoly> fetchTags(String storage) {
        SQLiteStorage sqLiteStorage = sqLiteStorage(storage);
        try (Connection connection = sqLiteStorage.openDb()) {
            return sqLiteStorage.fetchTags(connection);
        } catch (SQLException e) {
            LOG.warn("Failed to fetchTags {} ", storage, e);
            throw new EmbeddedStorageException(e);
        }
    }

    public List<BasicPoly> fetchTagsIndex(String storage, String tag) {
        SQLiteStorage sqLiteStorage = sqLiteStorage(storage);
        try (Connection connection = sqLiteStorage.openDb()) {
            return sqLiteStorage.fetchTagIndex(connection, tag);
        } catch (SQLException e) {
            LOG.warn("Failed to fetchTagsIndex {} {} ", storage, tag, e);
            throw new EmbeddedStorageException(e);
        }
    }

    public Optional<BasicPoly> fetchPoly(String storage, String id) {
        SQLiteStorage sqLiteStorage = sqLiteStorage(storage);
        try (Connection connection = sqLiteStorage.openDb()) {
            return sqLiteStorage.fetchPoly(connection, id);
        } catch (SQLException e) {
            LOG.warn("Failed to fetchTagsIndex {} {} ", storage, id, e);
            throw new EmbeddedStorageException(e);
        }
    }

    public Map<String, BasicPoly> fetchPolyBatch(String storage, List<String> ids) {
        Map<String, BasicPoly> polyMap = new HashMap<>();
        SQLiteStorage sqLiteStorage = sqLiteStorage(storage);
        try (Connection connection = sqLiteStorage.openDb()) {
            for (String id : ids) {
                Optional<BasicPoly> basicPoly = sqLiteStorage.fetchPoly(connection, id);
                basicPoly.ifPresent( item -> polyMap.put(item._id(), item));
            }
        } catch (SQLException e) {
            LOG.warn("Failed to fetch basic polys {} {} ", storage, ids, e);
            throw new EmbeddedStorageException(e);
        }

        return polyMap;
    }

    private File fetchFile(String storage, String file) {
        return new File(fetchRootFile(storage), file);
    }

    private File fetchRootFile(String storage) {
        return new File(storageRoot, storage);
    }

    public String getStorageRoot() {
        return storageRoot;
    }

    public void setStorageRoot(String storageRoot) {
        this.storageRoot = storageRoot;
    }
}
