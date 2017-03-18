package com.unidev.polydata.exception;


import com.unidev.platform.common.exception.UnidevRuntimeException;

/**
 * Exception thrown if requested storage is not found
 */
public class NotFoundException extends UnidevRuntimeException {

    public NotFoundException() {
        super("Not found");
    }

    public NotFoundException(String message) {
        super(message);
    }
}
