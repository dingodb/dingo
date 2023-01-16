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
import io.dingodb.common.codec.DingoCodec;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.Part;
import io.dingodb.server.api.MetaApi;
import io.dingodb.server.api.MetaServiceApi;
import io.dingodb.server.protocol.meta.Executor;
import io.dingodb.web.mapper.DTOMapper;
import io.dingodb.web.model.dto.meta.ExecutorDTO;
import io.dingodb.web.model.dto.meta.PartDTO;
import io.dingodb.web.model.dto.meta.TableDTO;
import io.dingodb.web.model.dto.meta.TablePartDTO;
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

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.SERVICE_IDENTIFIER;
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
    private MetaService metaService;

    @Autowired
    private DTOMapper mapper;

    @ApiOperation("Get tables")
    @GetMapping("/table")
    public ResponseEntity<List<TableDTO>> getTables() {
        return ResponseEntity.ok(
            metaServiceApi.getTableMetas(metaService.id()).stream().map(mapper::mapping).collect(Collectors.toList())
        );
    }

    @ApiOperation("Get table")
    @GetMapping("/{table}/definition")
    public ResponseEntity<String> getTableDef(@PathVariable String table) {
        return ResponseEntity.ok(metaService.getTableDefinition(table).toString());
    }

    @ApiOperation("Get table")
    @GetMapping("/{table}")
    public ResponseEntity<TableDTO> getTable(@PathVariable String table) {
        return ResponseEntity.ok(
            mapper.mapping(metaServiceApi.getTableMeta(metaService.getTableId(table.toUpperCase())))
        );
    }

    @ApiOperation("Get table parts")
    @GetMapping("/{table}/parts")
    public ResponseEntity<List<PartDTO>> getTableParts(@PathVariable String table) {
        return ResponseEntity.ok(
            metaService.getParts(table).values().stream().map(mapper::mapping).collect(Collectors.toList())
        );
    }

    @ApiOperation("Get part by primary key.")
    @PostMapping("/{table}/pk/part")
    public ResponseEntity<TablePartDTO> getPartByPK(
        @PathVariable String table, @RequestBody Map<String, Object> params
    ) throws IOException {
        table = table.toUpperCase();
        params = params.entrySet().stream().collect(toMap(__ -> __.getKey().toUpperCase(), Map.Entry::getValue));
        NavigableMap<ComparableByteArray, Part> parts = metaService.getParts(table);
        TableDefinition def = metaApi.tableDefinition(table);
        Object[] keys = def.getKeyMapping().revMap((Object[]) def.getDingoType().parse(def.getColumns().stream()
            .map(ColumnDefinition::getName)
            .map(params::get)
            .toArray(Object[]::new)
        ));
        return ResponseEntity.ok(mapper.mapping(metaApi.tablePart(parts.floorEntry(
            new ComparableByteArray(new DingoCodec(def.getDingoSchemaOfKey(), null, true).encodeKey(keys))
        ).getValue().getId())));
    }

    @ApiOperation("Encode primary key.")
    @PostMapping("/{table}/pk/encode")
    public ResponseEntity<String> encodePK(
        @PathVariable String table, @RequestBody Map<String, Object> params
    ) throws IOException {
        table = table.toUpperCase();
        params = params.entrySet().stream().collect(toMap(__ -> __.getKey().toUpperCase(), Map.Entry::getValue));
        TableDefinition def = metaApi.tableDefinition(table);
        Object[] keys = def.getKeyMapping().revMap((Object[]) def.getDingoType().parse(def.getColumns().stream()
            .map(ColumnDefinition::getName)
            .map(params::get)
            .toArray(Object[]::new)
        ));
        return ResponseEntity.ok(mapper.mapping(new DingoCodec(def.getDingoSchemaOfKey(), null, true).encodeKey(keys)));
    }

    @ApiOperation("Decode primary key.")
    @PostMapping("/{table}/pk/decode")
    public ResponseEntity<Map<String, Object>> decodePK(
        @PathVariable String table, @RequestBody int[] input
    ) throws IOException {
        table = table.toUpperCase();
        TableDefinition def = metaApi.tableDefinition(table);
        Object[] keys = new DingoCodec(def.getDingoSchemaOfKey(), null, true).decodeKey(mapper.mapping(input));
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
            new CommonId(ID_TYPE.service, SERVICE_IDENTIFIER.executor, 0, id)
        )));
    }

}
