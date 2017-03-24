# polydata-storage-sqlite-hateoas

Example API calls

Fetch index:

`http://localhost:15200/api/storage/test`

```json
{"version":"0.0.1","data":null,"_links":{"tags":{"href":"http://localhost:15200/api/storage/test/tags"},"tag_index":{"href":"http://localhost:15200/api/storage/test/tag/id"},"query":{"href":"http://localhost:15200/api/storage/test/query"},"poly":{"href":"http://localhost:15200/api/storage/test/poly/id"}}}
```

Batch fetch polys

`curl -v -H 'Content-Type: application/json'   -X POST -d '["qwe", "9szigeqb"]' http://localhost:15200/api/storage/test/poly`

Query polys

`curl -X POST  http://localhost:15200/api/storage/test/query`

curl -v -H 'Content-Type: application/json'   -X POST -d '{ "randomOrder":"true", "page":"0" , "itemPerPage" : "1" }' http://localhost:15200/api/storage/test/query