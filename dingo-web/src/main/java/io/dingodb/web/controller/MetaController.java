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

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.server.api.MetaApi;
import io.dingodb.server.api.MetaServiceApi;
import io.dingodb.server.protocol.meta.Executor;
import io.dingodb.server.protocol.meta.TablePart;
import io.dingodb.web.mapper.DTOMapper;
import io.dingodb.web.model.dto.meta.ReplicaDTO;
import io.dingodb.web.model.dto.meta.TableDTO;
import io.dingodb.web.model.dto.meta.TablePartDTO;
import io.dingodb.web.model.dto.meta.TablePartStatsDTO;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.STATS_IDENTIFIER;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;

@Api("Meta")
@RestController
@RequestMapping("/meta")
public class MetaController {

    @Autowired
    private MetaApi metaApi;

    @Autowired
    private MetaServiceApi metaServiceApi;

    @Autowired
    private DTOMapper mapper;

    @ApiOperation("Get table")
    @GetMapping("/{table}/definition")
    public ResponseEntity<String> getTableDef(@PathVariable String table) {
        return ResponseEntity.ok(metaApi.tableDefinition(metaApi.tableId(table.toUpperCase())).toString());
    }

    @ApiOperation("Get table")
    @GetMapping("/{table}")
    public ResponseEntity<TableDTO> getTable(@PathVariable String table) {
        return ResponseEntity.ok(mapper.mapping(metaApi.table(metaApi.tableId(table.toUpperCase()))));
    }

    @ApiOperation("Get table parts")
    @GetMapping("/{table}/parts")
    public ResponseEntity<List<TablePartDTO>> getTableParts(@PathVariable String table) {
        return ResponseEntity.ok(
            metaApi.tableParts(table.toUpperCase()).stream()
                .map(mapper::mapping)
                .collect(Collectors.toList())
        );
    }

    @ApiOperation("Get table stats")
    @GetMapping("/{table}/stats")
    public ResponseEntity<List<TablePartStatsDTO>> getTableStats(@PathVariable String table) {
        return ResponseEntity.ok(
            metaApi.tableParts(metaApi.tableId(table.toUpperCase())).stream()
                .map(TablePart::getId)
                .map(metaApi::tablePartStats)
                .map(mapper::mapping)
                .collect(Collectors.toList())
        );
    }

    @ApiOperation("Get table stats")
    @GetMapping("/{table}/replicas")
    public ResponseEntity<Map<String, List<ReplicaDTO>>> getTableReplicas(@PathVariable String table) {
        List<TablePart> tableParts = metaApi.tableParts(table.toUpperCase());
        return ResponseEntity.ok(
            tableParts.stream().collect(Collectors.toMap(
                tp -> tp.getId().toString(),
                tp -> metaApi.replicas(tp.getId()).stream().map(mapper::mapping).collect(Collectors.toList())
            )));
    }

    @ApiOperation("Get table part stats")
    @GetMapping("/{table}/{part}/stats")
    public ResponseEntity<TablePartStatsDTO> getTablePartStats(@PathVariable String table, @PathVariable Integer part) {
        return ResponseEntity.ok(mapper.mapping(metaApi.tablePartStats(
            new CommonId(ID_TYPE.stats, STATS_IDENTIFIER.part, metaApi.tableId(table.toUpperCase()).seqContent(), part)
        )));
    }

    @ApiOperation("Get table part")
    @GetMapping("/{table}/{part}/part")
    public ResponseEntity<TablePartDTO> getTablePart(@PathVariable String table, @PathVariable Integer part) {
        return ResponseEntity.ok(mapper.mapping(metaApi.tablePart(
            new CommonId(ID_TYPE.stats, STATS_IDENTIFIER.part, metaApi.tableId(table.toUpperCase()).seqContent(), part)
        )));
    }

    @ApiOperation("Get table part")
    @GetMapping("/{table}/{part}/replica")
    public ResponseEntity<List<ReplicaDTO>> getTablePartReplica(
        @PathVariable String table, @PathVariable Integer part
    ) {
        return ResponseEntity.ok(metaApi
            .replicas(new CommonId(
                ID_TYPE.table,
                TABLE_IDENTIFIER.part,
                metaApi.tableId(table.toUpperCase()).seqContent(),
                part
            )).stream()
            .map(mapper::mapping)
            .collect(Collectors.toList())
        );
    }

    @ApiOperation("Get executor.")
    @GetMapping("/executor/{host}/{port}")
    public ResponseEntity<Executor> getExecutor(@PathVariable String host, @PathVariable Integer port) {
        return ResponseEntity.ok(metaApi.executor(new Location(host, port)));
    }

}
