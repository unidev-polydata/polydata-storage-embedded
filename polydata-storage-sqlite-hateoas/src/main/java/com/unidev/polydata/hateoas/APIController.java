package com.unidev.polydata.hateoas;

import com.netflix.governator.annotations.binding.Request;
import com.unidev.polydata.SQLitePolyQuery;
import com.unidev.polydata.StorageService;
import com.unidev.polydata.domain.BasicPoly;
import com.unidev.polydata.domain.bucket.BasicPolyBucket;
import com.unidev.polydata.exception.NotFoundException;
import com.unidev.polydata.model.HateoasResponse;
import com.unidev.polydata.model.ListResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.hateoas.ResourceSupport;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

import static com.unidev.polydata.model.HateoasResponse.hateoasResponse;
import static org.springframework.hateoas.mvc.ControllerLinkBuilder.linkTo;

/**
 * API requests controller
 */
@RequestMapping("/api")
@RestController
public class APIController {

    private static Logger LOG = LoggerFactory.getLogger(APIController.class);

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
                linkTo(APIController.class).slash("storage").slash(storage).slash("tags").withRel("tags"),
                linkTo(APIController.class).slash("storage").slash(storage).slash("tag").slash("id").withRel("tag_index"),
                linkTo(APIController.class).slash("storage").slash(storage).slash("query").withRel("query"),
                linkTo(APIController.class).slash("storage").slash(storage).slash("poly").slash("id").withRel("poly")
        );
        return hateoasPolyIndex;
    }


    @PostMapping(value = "/storage/{storage}/query", produces= MediaType.APPLICATION_JSON_VALUE)
    public HateoasResponse query(@PathVariable("storage") String storage, @RequestBody(required = false) SQLitePolyQuery polyQuery) {
        if (!storageService.existStorageRoot(storage)) {
            LOG.warn("Not found storage {}", storage);
            throw new NotFoundException("Storage " + storage + " not found");
        }
        if (polyQuery != null) {
            if (polyQuery.getItemPerPage() > 256) {
                polyQuery.setItemPerPage(SQLitePolyQuery.DEFAULT_ITEM_PER_PAGE);
            }
            if (polyQuery.getPage() < 0) {
                polyQuery.setPage(0L);
            }
        } else {
            polyQuery = new SQLitePolyQuery();
        }

        ListResponse listResponse = storageService.queryPoly(storage, polyQuery);
        return listResponse;
    }

    @GetMapping(value = "/storage/{storage}/tags", produces= MediaType.APPLICATION_JSON_VALUE)
    public HateoasResponse tags(@PathVariable("storage") String storage) {
        if (!storageService.existStorageRoot(storage)) {
            LOG.warn("Not found storage {}", storage);
            throw new NotFoundException("Storage " + storage + " not found");
        }
        List<BasicPoly> tags = storageService.fetchTags(storage);
        return hateoasResponse().data(tags);
    }

    @GetMapping(value = "/storage/{storage}/tag/{tag}", produces= MediaType.APPLICATION_JSON_VALUE)
    public HateoasResponse tagIndex(@PathVariable("storage") String storage, @PathVariable("tag") String tag) {
        if (!storageService.existStorageRoot(storage)) {
            LOG.warn("Not found storage {}", storage);
            throw new NotFoundException("Storage " + storage + " not found");
        }
        List<BasicPoly> tags = storageService.fetchTagsIndex(storage, tag);
        return hateoasResponse().data(tags);
    }

    @GetMapping(value = "/storage/{storage}/poly/{id}", produces= MediaType.APPLICATION_JSON_VALUE)
    public HateoasResponse  poly(@PathVariable("storage") String storage, @PathVariable("id") String id) {
        if (!storageService.existStorageRoot(storage)) {
            LOG.warn("Not found storage {}", storage);
            throw new NotFoundException("Storage " + storage + " not found");
        }
        Optional<BasicPoly> poly = storageService.fetchPoly(storage, id);
        if (!poly.isPresent()) {
            throw new NotFoundException("Poly not found " + id + " in storage " + storage);
        }
        return hateoasResponse().data(poly.get());
    }
}
