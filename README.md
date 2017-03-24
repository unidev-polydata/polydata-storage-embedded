# polydata-storage-sqlite

SQLite storage for polydata records

## DB records

*Data*

| id | _id  | tags  | data  |
|---|---|---|---|
| 1  |  ABC  | [ {tag:potato}, {category:tomato}] |  { }  |

*Tag*

| id | _id  | count  | data  |
|---|---|---|---|
| 1  |  tag_name  | 11 |  { }  |

*Tag Index*

tag_index_*category*

| id | _id  | tag  | data  |
|---|---|---|---|
| 1  |  document_id  | cats_category |  { }  |



## Modules

`polydata-storage-sqlite` - module which works with one instance of db

`polydata-storage-sqlite-hateoas` - hateoas interface for accessing poly records

*unused/deprecated*  `polydata-manager-sqlite` - module for management of multiple db instances

## Usage

```
compile 'com.unidev.polydata:polydata-storage-sqlite:2.0.0-SNAPSHOT'
```

License
=======
 
    Copyright (c) 2016 Denis O <denis@universal-development.com>
 
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
 
       http://www.apache.org/licenses/LICENSE-2.0
 
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

