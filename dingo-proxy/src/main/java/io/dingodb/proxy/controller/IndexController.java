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

package io.dingodb.proxy.controller;

import io.dingodb.client.DingoClient;
import io.dingodb.client.common.IndexDefinition;
import io.dingodb.client.vector.VectorClient;
import io.dingodb.proxy.Result;
import io.dingodb.proxy.mapper.EntityMapper;
import io.dingodb.sdk.service.entity.meta.IndexMetrics;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Api("Index")
@RestController
@RequestMapping("/index")
public class IndexController {

    @Autowired
    private DingoClient dingoClient;

    @Autowired
    private VectorClient vectorClient;

    @Autowired
    private EntityMapper mapper;

    @ApiOperation("Create index")
    @PostMapping("/api/{schema}")
    public Result<Boolean> crateIndex(@PathVariable String schema, @RequestBody IndexDefinition definition) {
        try {
            if (!definition.getIsAutoIncrement()) {
                definition.setAutoIncrement(0L);
            }
            return Result.ok(vectorClient.createIndex(schema, definition));
        } catch (Exception e) {
            log.error("Create index error.", e);
            return Result.errorMsg(e.getMessage());
        }
    }

    @ApiOperation("Update hnsw max_elements")
    @PutMapping("/api/{schema}/{index}/{maxElements}")
    public Result<Boolean> updateIndex(@PathVariable String schema, @PathVariable String index, @PathVariable Integer maxElements) {
        try {
            return Result.ok(vectorClient.updateMaxElements(schema, index, maxElements));
        } catch (Exception e) {
            log.error("Update index max elements error.", e);
            return Result.errorMsg(e.getMessage());
        }
    }

    @ApiOperation("Drop index")
    @DeleteMapping("/api/{schema}/{index}")
    public Result<Boolean> deleteIndex(@PathVariable String schema, @PathVariable String index) {
        try {
            return Result.ok(vectorClient.dropIndex(schema, index));
        } catch (Exception e) {
            log.error("Drop index error.", e);
            return Result.errorMsg(e.getMessage());
        }
    }

    @ApiOperation("Get index")
    @GetMapping("/api/{schema}/{index}")
    public Result<IndexDefinition> get(@PathVariable String schema, @PathVariable String index) {
        try {
            return Result.ok(vectorClient.getIndex(schema, index));
        } catch (Exception e) {
            log.error("Get index error.", e);
            return Result.errorMsg(e.getMessage());
        }
    }

    @ApiOperation("Get index metrics")
    @GetMapping("/api/{schema}/{index}/metrics")
    public Result<IndexMetrics> getIndexMetrics(@PathVariable String schema, @PathVariable String index) {
        try {
            return Result.ok(vectorClient.getIndexMetrics(schema, index));
        } catch (Exception e) {
            log.error("Get index metrics error.", e);
            return Result.errorMsg(e.getMessage());
        }
    }

    @ApiOperation("Get index names")
    @GetMapping("/api/{schema}")
    public Result<List<String>> getNames(@PathVariable String schema) {
        try {
            return Result.ok(vectorClient.getIndexes(schema).stream().map(IndexDefinition::getName).collect(Collectors.toList()));
        } catch (Exception e) {
            log.error("Get index names error.", e);
            return Result.errorMsg(e.getMessage());
        }
    }
}
