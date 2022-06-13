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
import io.dingodb.server.api.ScheduleApi;
import io.dingodb.server.protocol.meta.Executor;
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

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.TABLE_IDENTIFIER;

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
        scheduleApi.maxSize(tableId(table), value);
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
    @PostMapping("/{table}/{part}/split")
    public ResponseEntity<String> splitPart(
        @PathVariable String table, @PathVariable Integer part, @RequestBody KeyDTO key
    ) {
        CommonId tableId = metaApi.tableId(table.toUpperCase());
        CommonId partId = new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.part, tableId.seqContent(), part);
        scheduleApi.splitPart(tableId, partId, mapper.mapping(key.getKey()));
        return ResponseEntity.ok("ok");
    }

}
