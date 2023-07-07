/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.web.controller;

import io.dingodb.client.DingoClient;
import io.dingodb.client.common.IndexDefinition;
import io.dingodb.sdk.common.index.Index;
import io.dingodb.web.mapper.EntityMapper;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Api("Index")
@RestController
@RequestMapping("/index")
public class IndexController {

    @Autowired
    private DingoClient dingoClient;

    @Autowired
    private EntityMapper mapper;

    @ApiOperation("Create index")
    @PutMapping("/api/{schema}")
    public ResponseEntity<Boolean> crateIndex(@PathVariable String schema, @RequestBody IndexDefinition definition) {
        return ResponseEntity.ok(dingoClient.createIndex(schema, definition.getName(), definition));
    }

    @ApiOperation("Drop index")
    @DeleteMapping("/api/{schema}/{index}")
    public ResponseEntity<Boolean> deleteIndex(@PathVariable String schema, @PathVariable String index) {
        return ResponseEntity.ok(dingoClient.dropIndex(schema, index));
    }

    @ApiOperation("Get index")
    @GetMapping("/api/{schema}/{index}")
    public ResponseEntity<Index> get(@PathVariable String schema, @PathVariable String index) {
        return ResponseEntity.ok(dingoClient.getIndex(schema, index));
    }
}
