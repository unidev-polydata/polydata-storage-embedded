package com.unidev.polydata;

import com.unidev.platform.common.exception.UnidevRuntimeException;
import com.unidev.polydata.domain.bucket.BasicPolyBucket;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileReader;

/**
 * Service for managing poly records
 */
@Service
public class StorageService {

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
            return SQLitePolyConstants.POLY_OBJECT_MAPPER.readValue(fileReader, BasicPolyBucket.class);
        } catch (Exception e) {
            throw new UnidevRuntimeException(e);
        }
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
