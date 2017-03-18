package com.unidev.polydata;


import com.unidev.platform.common.dto.response.Response;
import com.unidev.polydata.exception.NotFoundException;
import org.apache.catalina.servlet4preview.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Handling of different types of exceptions
 */
@ControllerAdvice
public class ErrorHandler {

    private static Logger LOG = LoggerFactory.getLogger(ErrorHandler.class);


    @ExceptionHandler(Throwable.class)
    @ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR)
    @ResponseBody
    public Response serviceError(HttpServletRequest req, Throwable e) {
        LOG.error("Error in handling request {}", req, e);
        Response<String, String> response = new Response<>();
        response.setPayload("Service error");
        response.setStatus("service-error");
        return response;
    }

    @ExceptionHandler(NotFoundException.class)
    @ResponseStatus(value = HttpStatus.NOT_FOUND)
    @ResponseBody
    public Response handleNotFoundError(HttpServletRequest req) {
        LOG.error("Not found exception for request {}", req);
        Response<String, String> response = new Response<>();
        response.setPayload("Not found");
        response.setStatus("not-found");
        return response;
    }

}
