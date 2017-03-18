package com.unidev.polydata.model;


import com.unidev.polydata.domain.BasicPoly;

import java.util.List;


/**
 * Model class for holding list of polys
 */
public class ListResponse {

    private List<BasicPoly> list;
    private long count;

    public List<BasicPoly> getList() {
        return list;
    }

    public void setList(List<BasicPoly> list) {
        this.list = list;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
