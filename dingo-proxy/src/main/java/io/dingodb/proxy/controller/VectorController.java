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

import io.dingodb.client.common.VectorScanQuery;
import io.dingodb.client.vector.VectorClient;
import io.dingodb.proxy.Result;
import io.dingodb.proxy.mapper.EntityMapper;
import io.dingodb.proxy.model.dto.VectorDistanceArray;
import io.dingodb.proxy.model.dto.VectorGet;
import io.dingodb.proxy.model.dto.VectorSearch;
import io.dingodb.proxy.model.dto.VectorWithDistance;
import io.dingodb.proxy.model.dto.VectorWithId;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.service.entity.common.VectorIndexMetrics;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Api("Vector")
@Slf4j
@RestController
@RequestMapping("/vector")
public class VectorController {

    @Autowired
    private VectorClient vectorClient;

    @Autowired
    private EntityMapper mapper;

    @ApiOperation("Vector add")
    @PutMapping("/api/{schema}/{index}")
    public Result<List<VectorWithId>> vectorAdd(
        @PathVariable String schema,
        @PathVariable String index,
        @RequestBody List<VectorWithId> vectors) {
        try {
            List<io.dingodb.sdk.service.entity.common.VectorWithId> result = vectorClient
                .vectorAdd(schema, index, vectors.stream()
                    .map(mapper::mapping)
                    .collect(Collectors.toList())
                );
            return Result.ok(result.stream().map(mapper::mapping).collect(Collectors.toList()));
        } catch (Exception e) {
            List<VectorWithId> result = new ArrayList<>();
            vectors.forEach(v -> result.add(null));
            log.error("Vector add error.", e);
            return Result.build(500, e.getMessage(), result);
        }
    }

    @ApiOperation("Vector upsert")
    @PutMapping("/api/{schema}/{index}/upsert")
    public Result<List<VectorWithId>> vectorUpsert(
        @PathVariable String schema,
        @PathVariable String index,
        @RequestBody List<VectorWithId> vectors) {
        try {
            List<io.dingodb.sdk.service.entity.common.VectorWithId> result = vectorClient
                .vectorUpsert(schema, index, vectors.stream()
                    .map(mapper::mapping)
                    .collect(Collectors.toList())
                );
            return Result.ok(result.stream().map(mapper::mapping).collect(Collectors.toList()));
        } catch (Exception e) {
            List<VectorWithId> result = new ArrayList<>();
            vectors.forEach(v -> result.add(null));
            log.error("Vector upsert error.", e);
            return Result.build(500, e.getMessage(), result);
        }
    }

    @ApiOperation("Vector delete")
    @DeleteMapping("/api/{schema}/{index}")
    public Result<List<Boolean>> vectorDelete(
        @PathVariable String schema,
        @PathVariable String index,
        @RequestBody List<Long> ids) {
        try {
            long count = ids.stream().distinct().count();
            if (ids.size() != count) {
                throw new DingoClientException("During the delete operation, duplicate ids are not allowed");
            }
            return Result.ok(vectorClient.vectorDelete(schema, index, ids));
        } catch (Exception e) {
            log.error("Vector delete error.", e);
            return Result.errorMsg(e.getMessage());
        }
    }

    @ApiOperation("Vector get")
    @PostMapping("/api/{schema}/{index}/get")
    public Result<List<VectorWithId>> vectorGet(
        @PathVariable String schema,
        @PathVariable String index,
        @RequestBody VectorGet vectorGet
        ) {
        try {
            List<Long> ids = vectorGet.getIds();
            Map<Long, io.dingodb.sdk.service.entity.common.VectorWithId> vectorWithIdMap = vectorClient.vectorBatchQuery(
                schema,
                index,
                new HashSet<>(vectorGet.getIds()),
                vectorGet.getWithoutVectorData(),
                vectorGet.getWithoutScalarData(),
                vectorGet.getKeys());
            return Result.ok(ids.stream().map(vectorWithIdMap::get).map(mapper::mapping).collect(Collectors.toList()));
        } catch (Exception e) {
            log.error("Vector get error.", e);
            return Result.errorMsg(e.getMessage());
        }
    }

    @ApiOperation("Get max vector id")
    @GetMapping("/api/{schema}/{index}/id")
    public Result<Long> vectorMaxId(
        @PathVariable String schema,
        @PathVariable String index,
        Boolean isGetMin
    ) {
        try {
            return Result.ok(vectorClient.vectorGetBorderId(schema, index, isGetMin));
        } catch (Exception e) {
            log.error("Vector get max error.", e);
            return Result.errorMsg(e.getMessage());
        }
    }

    @ApiOperation("Vector search")
    @PostMapping("/api/{schema}/{index}")
    public Result<List<VectorDistanceArray>> vectorSearch(
        @PathVariable String schema,
        @PathVariable String index,
        @RequestBody VectorSearch vectorSearch) {
        try {
            return Result.ok(vectorClient.vectorSearch(
                schema, index, mapper.mapping(vectorSearch)
            ).stream().map(mapper::mapping).collect(Collectors.toList()));
        } catch (Exception e) {
            log.error("Vector search error.", e);
            return Result.errorMsg(e.getMessage());
        }
    }

    @ApiOperation("Vector scan query")
    @PostMapping("/api/{schema}/{index}/scan")
    public Result<List<VectorWithId>> vectorScanQuery(
        @PathVariable String schema,
        @PathVariable String index,
        @RequestBody VectorScanQuery vectorScanQuery
    ) {
        try {
            return Result.ok(vectorClient.vectorScanQuery(schema, index, vectorScanQuery)
                .stream()
                .map(mapper::mapping)
                .collect(Collectors.toList()));
        } catch (Exception e) {
            log.error("Vector scan error.", e);
            return Result.errorMsg(e.getMessage());
        }
    }

    @ApiOperation("Vector get region metrics")
    @GetMapping("/api/{schema}/{index}")
    public Result<VectorIndexMetrics> vectorGetMetrics(@PathVariable String schema, @PathVariable String index) {
        try {
            return Result.ok(vectorClient.getRegionMetrics(schema, index));
        } catch (Exception e) {
            log.error("Vector region metrics error.", e);
            return Result.errorMsg(e.getMessage());
        }
    }

    @ApiOperation("Vector count")
    @GetMapping("/api/{schema}/{index}/count")
    public Result<Long> vectorCount(@PathVariable String schema, @PathVariable String index) {
        try {
            return Result.ok(vectorClient.vectorCount(schema, index));
        } catch (Exception e) {
            log.error("Vector count error.", e);
            return Result.errorMsg(e.getMessage());
        }
    }
}
