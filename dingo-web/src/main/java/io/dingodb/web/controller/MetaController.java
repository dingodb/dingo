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
import io.dingodb.common.codec.AvroCodec;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.meta.Part;
import io.dingodb.server.api.MetaApi;
import io.dingodb.server.api.MetaServiceApi;
import io.dingodb.server.protocol.meta.Executor;
import io.dingodb.server.protocol.meta.TablePart;
import io.dingodb.web.mapper.DTOMapper;
import io.dingodb.web.model.dto.meta.ExecutorDTO;
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
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.stream.Collectors;

import static io.dingodb.common.codec.PrimitiveCodec.encodeInt;
import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.SERVICE_IDENTIFIER;
import static io.dingodb.server.protocol.CommonIdConstant.STATS_IDENTIFIER;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;
import static java.util.stream.Collectors.toMap;

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

    @ApiOperation("Get tables")
    @GetMapping("/table")
    public ResponseEntity<List<TableDTO>> getTables() {
        return ResponseEntity.ok(metaApi.table().stream().map(mapper::mapping).collect(Collectors.toList()));
    }

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
            tableParts.stream().collect(toMap(
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
            new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.part, metaApi.tableId(table.toUpperCase()).seqContent(), part)
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

    @ApiOperation("Get part by primary key.")
    @PostMapping("/{table}/pk/part")
    public ResponseEntity<TablePartDTO> getPartByPK(
        @PathVariable String table, @RequestBody Map<String, Object> params
    ) throws IOException {
        table = table.toUpperCase();
        params = params.entrySet().stream().collect(toMap(__ -> __.getKey().toUpperCase(), Map.Entry::getValue));
        NavigableMap<ComparableByteArray, Part> parts = metaServiceApi.getParts(table);
        TableDefinition def = metaApi.tableDefinition(table);
        Object[] keys = def.getKeyMapping().revMap(def.getTupleSchema().parse(def.getColumns().stream()
            .map(ColumnDefinition::getName)
            .map(params::get)
            .toArray(Object[]::new)
        ));
        return ResponseEntity.ok(mapper.mapping(metaApi.tablePart(CommonId.decode(parts.floorEntry(
            new ComparableByteArray(new AvroCodec(def.getAvroSchemaOfKey()).encode(keys))
        ).getValue().getId()))));
    }

    @ApiOperation("Encode primary key.")
    @PostMapping("/{table}/pk/encode")
    public ResponseEntity<String> encodePK(
        @PathVariable String table, @RequestBody Map<String, Object> params
    ) throws IOException {
        table = table.toUpperCase();
        params = params.entrySet().stream().collect(toMap(__ -> __.getKey().toUpperCase(), Map.Entry::getValue));
        NavigableMap<ComparableByteArray, Part> parts = metaServiceApi.getParts(table);
        TableDefinition def = metaApi.tableDefinition(table);
        Object[] keys = def.getKeyMapping().revMap(def.getTupleSchema().parse(def.getColumns().stream()
            .map(ColumnDefinition::getName)
            .map(params::get)
            .toArray(Object[]::new)
        ));
        return ResponseEntity.ok(mapper.mapping(new AvroCodec(def.getAvroSchemaOfKey()).encode(keys)));
    }

    @ApiOperation("Decode primary key.")
    @PostMapping("/{table}/pk/decode")
    public ResponseEntity<Map<String, Object>> decodePK(
        @PathVariable String table, @RequestBody int[] input
    ) throws IOException {
        table = table.toUpperCase();
        TableDefinition def = metaApi.tableDefinition(table);
        Object[] keys = new AvroCodec(def.getAvroSchemaOfKey()).decode(mapper.mapping(input));
        int[] mappings = def.getKeyMapping().getMappings();
        Map<String, Object> result = new HashMap<>();
        for (int i = 0; i < mappings.length; i++) {
            result.put(def.getColumn(mappings[i]).getName(), keys[i]);
        }
        return ResponseEntity.ok(result);
    }

    @ApiOperation("Get executor.")
    @GetMapping("/executor/{host}/{port}")
    public ResponseEntity<Executor> getExecutor(@PathVariable String host, @PathVariable Integer port) {
        return ResponseEntity.ok(metaApi.executor(new Location(host, port)));
    }

    @ApiOperation("Get executor.")
    @GetMapping("/executor/{id}")
    public ResponseEntity<ExecutorDTO> getExecutor(@PathVariable Integer id) {
        return ResponseEntity.ok(mapper.mapping(metaApi.executor(
            new CommonId(ID_TYPE.service, SERVICE_IDENTIFIER.executor, encodeInt(0), id)
        )));
    }

}
