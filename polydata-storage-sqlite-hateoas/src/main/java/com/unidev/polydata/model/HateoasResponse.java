package com.unidev.polydata.model;

import com.unidev.polydata.Application;
import org.springframework.hateoas.ResourceSupport;

/**
 * Generic hataeos response object
 * @param <T>
 */
public class HateoasResponse<T> extends ResourceSupport {

    String version = Application.VERSION;
    T data;

    public static HateoasResponse hateoasResponse() {
        return new HateoasResponse();
    }


    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public HateoasResponse data(T data) {
        this.data = data;
        return this;
    }
}