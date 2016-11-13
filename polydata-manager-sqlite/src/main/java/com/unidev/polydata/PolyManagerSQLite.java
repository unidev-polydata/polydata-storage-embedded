package com.unidev.polydata;

import com.unidev.polydata.domain.Poly;
import com.unidev.polydata.manager.PolyManager;
import com.unidev.polydata.service.PolyService;
import com.unidev.polydata.storage.PolyStorage;

import java.util.HashMap;
import java.util.Map;

/**
 * SQLite polydata manager
 */
public class PolyManagerSQLite implements PolyManager {

    private SQLiteStorage polyStorage;
    private Map<String, PolyService> services = new HashMap<>();

    public PolyManagerSQLite(SQLiteStorage sqLiteStorage) {
        this.polyStorage = sqLiteStorage;
    }

    @Override
    public SQLiteStorage storage() {
        return polyStorage;
    }

    @Override
    public Map<String, PolyService> services() {
        return services;
    }

    public void registerService(PolyService polyService) {
        services.put(polyService.name(), polyService);
    }

    @Override
    public <P extends Poly> P persist(P poly) {
        Poly existingPoly = polyStorage.fetchById(poly._id());

        polyStorage.persist(poly);

        SQLitePolyServiceVO sqLitePolyServiceVO = new SQLitePolyServiceVO();
        sqLitePolyServiceVO.setPoly(poly);
        sqLitePolyServiceVO.setSqLiteStorage(polyStorage);

        services.values().forEach( service -> {
            if (existingPoly == null) {
                service.polyAdded(sqLitePolyServiceVO);
            } else {
                service.polyRemoved(sqLitePolyServiceVO);
            }
        });

        return poly;
    }

    @Override
    public <P extends Poly> void remove(P poly) {
        polyStorage.remove(poly._id());

        SQLitePolyServiceVO sqLitePolyServiceVO = new SQLitePolyServiceVO();
        sqLitePolyServiceVO.setPoly(poly);
        sqLitePolyServiceVO.setSqLiteStorage(polyStorage);

        services.values().forEach( service -> service.polyRemoved(sqLitePolyServiceVO));
    }
}
