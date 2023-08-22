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
import io.dingodb.client.common.VectorDistanceArray;
import io.dingodb.client.common.VectorSearch;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.vector.VectorCalcDistance;
import io.dingodb.sdk.common.vector.VectorDistanceRes;
import io.dingodb.sdk.common.vector.VectorIndexMetrics;
import io.dingodb.sdk.common.vector.VectorScanQuery;
import io.dingodb.web.mapper.EntityMapper;
import io.dingodb.web.model.dto.VectorGet;
import io.dingodb.web.model.dto.VectorWithId;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Api("Vector")
@RestController
@RequestMapping("/vector")
public class VectorController {

    @Autowired
    private DingoClient dingoClient;

    @Autowired
    private EntityMapper mapper;

    @ApiOperation("Vector add")
    @PutMapping("/api/{schema}/{index}")
    public ResponseEntity<List<VectorWithId>> vectorAdd(
        @PathVariable String schema,
        @PathVariable String index,
        @RequestBody List<VectorWithId> vectors) {
        List<io.dingodb.client.common.VectorWithId> result = dingoClient.vectorAdd(schema, index, vectors.stream()
            .map(mapper::mapping)
            .collect(Collectors.toList()));
        return ResponseEntity.ok(result.stream().map(mapper::mapping).collect(Collectors.toList()));
    }

    @ApiOperation("Vector delete")
    @DeleteMapping("/api/{schema}/{index}")
    public ResponseEntity<List<Boolean>> vectorDelete(
        @PathVariable String schema,
        @PathVariable String index,
        @RequestBody List<Long> ids) {
        long count = ids.stream().distinct().count();
        if (ids.size() != count) {
            throw new DingoClientException("During the delete operation, duplicate ids are not allowed");
        }
        return ResponseEntity.ok(dingoClient.vectorDelete(schema, index, ids));
    }

    @ApiOperation("Vector get")
    @PostMapping("/api/{schema}/{index}/get")
    public ResponseEntity<List<io.dingodb.client.common.VectorWithId>> vectorGet(
        @PathVariable String schema,
        @PathVariable String index,
        @RequestBody VectorGet vectorGet
        ) {
        List<Long> ids = vectorGet.getIds();
        Map<Long, io.dingodb.client.common.VectorWithId> vectorWithIdMap = dingoClient.vectorBatchQuery(
            schema,
            index,
            new HashSet<>(vectorGet.getIds()),
            vectorGet.getWithoutVectorData(),
            vectorGet.getWithScalarData(),
            vectorGet.getKeys());
        return ResponseEntity.ok(ids.stream().map(vectorWithIdMap::get).collect(Collectors.toList()));
    }

    @ApiOperation("Get max vector id")
    @GetMapping("/api/{schema}/{index}/id")
    public ResponseEntity<Long> vectorMaxId(
        @PathVariable String schema,
        @PathVariable String index,
        Boolean isGetMin
    ) {
        return ResponseEntity.ok(dingoClient.vectorGetBorderId(schema, index, isGetMin));
    }

    @ApiOperation("Vector search")
    @PostMapping("/api/{schema}/{index}")
    public ResponseEntity<List<VectorDistanceArray>> vectorSearch(
        @PathVariable String schema,
        @PathVariable String index,
        @RequestBody VectorSearch vectorSearch) {
        return ResponseEntity.ok(dingoClient.vectorSearch(schema, index, vectorSearch));
    }

    @ApiOperation("Vector scan query")
    @PostMapping("/api/{schema}/{index}/scan")
    public ResponseEntity<List<VectorWithId>> vectorScanQuery(
        @PathVariable String schema,
        @PathVariable String index,
        @RequestBody VectorScanQuery vectorScanQuery) {
        return ResponseEntity.ok(dingoClient.vectorScanQuery(schema, index, vectorScanQuery)
            .stream()
            .map(mapper::mapping)
            .collect(Collectors.toList()));
    }

    @ApiOperation("Vector calc distance")
    @PostMapping("/api/{schema}/{index}/calc")
    public ResponseEntity<VectorDistanceRes> vectorCalcDistance(
        @PathVariable String schema,
        @PathVariable String index,
        @RequestBody VectorCalcDistance calcDistance) {
        return ResponseEntity.ok(dingoClient.vectorCalcDistance(schema, index, calcDistance));
    }

    @ApiOperation("Vector get region metrics")
    @GetMapping("/api/{schema}/{index}")
    public ResponseEntity<VectorIndexMetrics> vectorGetMetrics(@PathVariable String schema, @PathVariable String index) {
        return ResponseEntity.ok(dingoClient.getRegionMetrics(schema, index));
    }
}
