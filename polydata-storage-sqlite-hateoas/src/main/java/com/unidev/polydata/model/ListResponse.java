package com.unidev.polydata.model;


import com.unidev.polydata.domain.BasicPoly;

import java.util.List;


/**
 * Model class for holding list of polys
 */
public class ListResponse extends HateoasResponse<List<BasicPoly>> {

    private long count;

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
