package com.unidev.polydata.hateoas;

import com.unidev.polydata.StorageService;
import com.unidev.polydata.domain.bucket.BasicPolyBucket;
import com.unidev.polydata.exception.NotFoundException;
import com.unidev.polydata.model.HateoasResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.hateoas.ResourceSupport;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import static com.unidev.polydata.model.HateoasResponse.hateoasResponse;
import static org.springframework.hateoas.mvc.ControllerLinkBuilder.linkTo;

@RestController("/api")
public class IndexController {

    private static Logger LOG = LoggerFactory.getLogger(IndexController.class);

    @Autowired
    private StorageService storageService;

    @GetMapping(value = "/storage/{storage}", produces= MediaType.APPLICATION_JSON_VALUE)
    public ResourceSupport index(@PathVariable("storage") String storage) {
        if (!storageService.existStorageRoot(storage)) {
            LOG.warn("Not found storage {}", storage);
            throw new NotFoundException("Storage " + storage + " not found");
        }
        HateoasResponse hateoasPolyIndex = hateoasResponse();

        if (storageService.existPolyBucket(storage)) {
            BasicPolyBucket basicPolyBucket = storageService.polyBucket(storage);
            hateoasPolyIndex.setData(basicPolyBucket);
        }


        hateoasPolyIndex.add(
                linkTo(IndexController.class).slash("storage").slash(storage).slash("tags").withRel("tags"),
                linkTo(IndexController.class).slash("storage").slash(storage).slash("query").withRel("query")
        );
        return hateoasPolyIndex;
    }
}
