package com.unidev.polydata;

import com.unidev.polydata.domain.Poly;
import com.unidev.polydata.service.ServicePolyVO;

/**
 * Value object for sqlite storage
 */
public class SQLitePolyServiceVO implements ServicePolyVO {

    private SQLiteStorage sqLiteStorage;
    private Poly poly;

    public SQLitePolyServiceVO() {}

    public SQLitePolyServiceVO(SQLiteStorage sqLiteStorage, Poly poly) {
        this.sqLiteStorage = sqLiteStorage;
        this.poly = poly;
    }

    @Override
    public Poly poly() {
        return poly;
    }

    public SQLiteStorage getSqLiteStorage() {
        return sqLiteStorage;
    }

    public void setSqLiteStorage(SQLiteStorage sqLiteStorage) {
        this.sqLiteStorage = sqLiteStorage;
    }

    public Poly getPoly() {
        return poly;
    }

    public void setPoly(Poly poly) {
        this.poly = poly;
    }
}
