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
import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.meta.Part;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.MetaApi;
import io.dingodb.server.api.MetaServiceApi;
import io.dingodb.server.api.ScheduleApi;
import io.dingodb.server.api.StoreReportStatsApi;
import io.dingodb.server.protocol.meta.Executor;
import io.dingodb.server.protocol.meta.Replica;
import io.dingodb.server.protocol.meta.TablePart;
import io.dingodb.web.mapper.DTOMapper;
import io.dingodb.web.model.dto.meta.KeyDTO;
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
import java.util.Map;
import java.util.NavigableMap;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;
import static java.util.stream.Collectors.toMap;

@Api("Schedule")
@RestController
@RequestMapping("/schedule")
public class ScheduleController {

    @Autowired private MetaApi metaApi;

    @Autowired private MetaServiceApi metaServiceApi;

    @Autowired private ScheduleApi scheduleApi;

    @Autowired private DTOMapper mapper;

    private CommonId tableId(String name) {
        return metaApi.tableId(name.toUpperCase());
    }

    @ApiOperation("Open auto split.")
    @GetMapping("/{table}/auto/open")
    public ResponseEntity<String> openAuto(@PathVariable String table) {
        scheduleApi.autoSplit(tableId(table), true);
        return ResponseEntity.ok("ok");
    }

    @ApiOperation("Close auto split.")
    @GetMapping("/{table}/auto/close")
    public ResponseEntity<String> closeAuto(@PathVariable String table) {
        scheduleApi.autoSplit(tableId(table), false);
        return ResponseEntity.ok("ok");
    }

    @ApiOperation("Set auto size.")
    @GetMapping("/{table}/auto/size/{value}")
    public ResponseEntity<String> setAutoSize(@PathVariable String table, @PathVariable Long value) {
        scheduleApi.maxSize(tableId(table), value);
        return ResponseEntity.ok("ok");
    }

    @ApiOperation("Set auto size.")
    @GetMapping("/{table}/auto/count/{value}")
    public ResponseEntity<String> setAutoCount(@PathVariable String table, @PathVariable Long value) {
        scheduleApi.maxCount(tableId(table), value);
        return ResponseEntity.ok("ok");
    }

    @ApiOperation("Add replica.")
    @GetMapping("/{table}/replica/{part}/{host}/{port}/add")
    public ResponseEntity<String> addReplica(
        @PathVariable String table, @PathVariable Integer part, @PathVariable String host, @PathVariable Integer port
    ) {
        CommonId tableId = metaApi.tableId(table.toUpperCase());
        Executor executor = metaApi.executor(new Location(host, port));
        CommonId partId = new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.part, tableId.seqContent(), part);
        scheduleApi.addReplica(tableId, partId, executor.getId());
        return ResponseEntity.ok("ok");
    }

    @ApiOperation("Remove replica.")
    @GetMapping("/{table}/replica/{part}/{host}/{port}/remove")
    public ResponseEntity<String> removeReplica(
        @PathVariable String table, @PathVariable Integer part, @PathVariable String host, @PathVariable Integer port
    ) {
        CommonId tableId = metaApi.tableId(table.toUpperCase());
        Executor executor = metaApi.executor(new Location(host, port));
        CommonId partId = new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.part, tableId.seqContent(), part);
        scheduleApi.removeReplica(tableId, partId, executor.getId());
        return ResponseEntity.ok("ok");
    }

    @ApiOperation("Transfer leader.")
    @GetMapping("/{table}/{part}/{host}/{port}/leader/transfer")
    public ResponseEntity<String> transferLeader(
        @PathVariable String table, @PathVariable Integer part, @PathVariable String host, @PathVariable Integer port
    ) {
        CommonId tableId = metaApi.tableId(table.toUpperCase());
        Executor executor = metaApi.executor(new Location(host, port));
        CommonId partId = new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.part, tableId.seqContent(), part);
        scheduleApi.transferLeader(tableId, partId, executor.getId());
        return ResponseEntity.ok("ok");
    }

    @ApiOperation("Split part.")
    @PostMapping("/{table}/{part}/split/bk")
    public ResponseEntity<String> splitPart(
        @PathVariable String table, @PathVariable Integer part, @RequestBody KeyDTO key
    ) {
        CommonId tableId = metaApi.tableId(table.toUpperCase());
        CommonId partId = new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.part, tableId.seqContent(), part);
        scheduleApi.splitPart(tableId, partId, mapper.mapping(key.getKey()));
        return ResponseEntity.ok("ok");
    }

    @ApiOperation("Split part.")
    @PostMapping("/{table}/{part}/split/pk")
    public ResponseEntity<String> splitPart(
        @PathVariable String table, @PathVariable Integer part, @RequestBody Map<String, Object> params
    ) throws IOException {
        table = table.toUpperCase();
        CommonId tableId = metaApi.tableId(table);
        CommonId partId = new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.part, tableId.seqContent(), part);
        params = params.entrySet().stream().collect(toMap(__ -> __.getKey().toUpperCase(), Map.Entry::getValue));
        NavigableMap<ComparableByteArray, Part> parts = metaServiceApi.getParts(table);
        TableDefinition def = metaApi.tableDefinition(table);
        Object[] keys = def.getKeyMapping().revMap(def.getTupleSchema().parse(def.getColumns().stream()
            .map(ColumnDefinition::getName)
            .map(params::get)
            .toArray(Object[]::new)
        ));
        scheduleApi.splitPart(tableId, partId, new AvroCodec(def.getAvroSchemaOfKey()).encode(keys));
        return ResponseEntity.ok("ok");
    }

    @ApiOperation("Split part.")
    @PostMapping("/{table}/split/bk")
    public ResponseEntity<String> splitPart(
        @PathVariable String table, @RequestBody KeyDTO key) {
        CommonId tableId = metaApi.tableId(table.toUpperCase());
        NavigableMap<ComparableByteArray, Part> parts = metaServiceApi.getParts(table);
        byte[] keys = mapper.mapping(key.getKey());
        CommonId partId = CommonId.decode(parts.floorEntry(new ComparableByteArray(keys)).getValue().getId());
        scheduleApi.splitPart(tableId, partId, keys);
        return ResponseEntity.ok("ok");
    }

    @ApiOperation("Split part.")
    @PostMapping("/{table}/split/pk")
    public ResponseEntity<String> splitPart(
        @PathVariable String table, @RequestBody Map<String, Object> params
    ) throws IOException {
        table = table.toUpperCase();
        CommonId tableId = metaApi.tableId(table);
        params = params.entrySet().stream().collect(toMap(__ -> __.getKey().toUpperCase(), Map.Entry::getValue));
        NavigableMap<ComparableByteArray, Part> parts = metaServiceApi.getParts(table);
        TableDefinition def = metaApi.tableDefinition(table);
        byte[] keys = new AvroCodec(def.getAvroSchemaOfKey()).encode(
            def.getKeyMapping().revMap(def.getTupleSchema().parse(def.getColumns().stream()
                .map(ColumnDefinition::getName)
                .map(params::get)
                .toArray(Object[]::new)
            )));
        CommonId partId = CommonId.decode(parts.floorEntry(new ComparableByteArray(keys)).getValue().getId());
        scheduleApi.splitPart(tableId, partId, keys);
        return ResponseEntity.ok("ok");
    }

    @ApiOperation("Open store report stats.")
    @GetMapping("/{table}/{part}/report/stats/open")
    public ResponseEntity<String> openStoreReportStats(@PathVariable String table, @PathVariable Integer part) {
        CommonId tableId = metaApi.tableId(table.toUpperCase());
        CommonId partId = new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.part, tableId.seqContent(), part);
        metaApi.replicas(partId).stream()
            .map(Replica::location)
            .map(__ -> ApiRegistry.getDefault().proxy(StoreReportStatsApi.class, () -> __))
            .forEach(api -> Executors.execute("open-report-stats", () -> api.open(tableId, partId)));
        return ResponseEntity.ok("ok");
    }

    @ApiOperation("Open store report stats for table.")
    @GetMapping("/{table}/report/stats/open")
    public ResponseEntity<String> openStoreReportStats(@PathVariable String table) {
        CommonId tableId = metaApi.tableId(table.toUpperCase());
        metaApi.tableParts(tableId).stream().map(TablePart::getId)
            .forEach(partId -> metaApi.replicas(partId).stream()
                .map(Replica::location)
                .map(__ -> ApiRegistry.getDefault().proxy(StoreReportStatsApi.class, () -> __))
                .forEach(api -> Executors.execute("open-report-stats", () -> api.open(tableId, partId))));
        return ResponseEntity.ok("ok");
    }

    @ApiOperation("Close store report stats.")
    @GetMapping("/{table}/{part}/report/stats/close")
    public ResponseEntity<String> closeStoreReportStats(@PathVariable String table, @PathVariable Integer part) {
        CommonId tableId = metaApi.tableId(table.toUpperCase());
        CommonId partId = new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.part, tableId.seqContent(), part);
        metaApi.replicas(partId).stream()
            .map(Replica::location)
            .map(__ -> ApiRegistry.getDefault().proxy(StoreReportStatsApi.class, () -> __))
            .forEach(api -> Executors.execute("close-report-stats", () -> api.close(tableId, partId)));
        return ResponseEntity.ok("ok");
    }

    @ApiOperation("Close store report stats for table.")
    @GetMapping("/{table}/report/stats/close")
    public ResponseEntity<String> closeStoreReportStats(@PathVariable String table) {
        CommonId tableId = metaApi.tableId(table.toUpperCase());
        metaApi.tableParts(tableId).stream().map(TablePart::getId)
            .forEach(partId -> metaApi.replicas(partId).stream()
                .map(Replica::location)
                .map(__ -> ApiRegistry.getDefault().proxy(StoreReportStatsApi.class, () -> __))
                .forEach(api -> Executors.execute("close-report-stats", () -> api.close(tableId, partId))));
        return ResponseEntity.ok("ok");
    }

}
