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
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.server.api.MetaApi;
import io.dingodb.server.api.MetaServiceApi;
import io.dingodb.server.protocol.meta.TablePart;
import io.dingodb.server.protocol.meta.TablePartStats;
import io.dingodb.web.model.Part;
import io.dingodb.web.model.PartReplicas;
import io.dingodb.web.model.PartStats;
import io.dingodb.web.model.Replica;
import io.dingodb.web.model.TableParts;
import io.dingodb.web.model.TableStats;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

    @ApiOperation("Get table")
    @GetMapping("/{table}/definition")
    public ResponseEntity<TableDefinition> getTableDef(@PathVariable Integer table) {
        return ResponseEntity.ok(metaApi.tableDefinition(
            new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.table, PrimitiveCodec.encodeInt(0), table)
        ));
    }

    @ApiOperation("Get table parts")
    @GetMapping("/{table}/parts")
    public ResponseEntity<TableParts> getTableParts(@PathVariable Integer table) {
        CommonId tableId = new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.table, PrimitiveCodec.encodeInt(0), table);
        List<Part> parts = metaApi.tableParts(tableId)
            .stream()
            .map(tp -> new Part(
                tp.getId().toString(),
                tp.getTable().toString(),
                Arrays.toString(tp.getStart()),
                Arrays.toString(tp.getEnd())
            ))
            .collect(Collectors.toList());
        return ResponseEntity.ok(new TableParts(tableId.toString(), parts));
    }

    @ApiOperation("Get table stats")
    @GetMapping("/{table}/stats")
    public ResponseEntity<TableStats> getTableStats(@PathVariable Integer table) {
        CommonId tableId = new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.table, PrimitiveCodec.encodeInt(0), table);
        List<PartStats> parts = metaApi.tableParts(tableId)
            .stream()
            .map(TablePart::getId)
            .map(metaApi::tablePartStats)
            .map(stats ->  new PartStats(
                stats.getId().toString(),
                stats.getTable().toString(),
                stats.getTablePart().toString(),
                stats.getLeader().toString(),
                stats.getApproximateStats().stream()
                    .map(as -> new PartStats.ApproximateStats(
                        Arrays.toString(as.getStartKey()),
                        Arrays.toString(as.getEndKey()),
                        as.getCount(),
                        as.getSize()
                    )).collect(Collectors.toList()),
                Collections.emptyList()
            ))
            .collect(Collectors.toList());
        return ResponseEntity.ok(new TableStats(tableId.toString(), parts));
    }

    @ApiOperation("Get table stats")
    @GetMapping("/{table}/replicas")
    public ResponseEntity<List<PartReplicas>> getTableReplicas(@PathVariable Integer table) {
        CommonId tableId = new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.table, PrimitiveCodec.encodeInt(0), table);
        List<PartReplicas> parts = metaApi.tableParts(tableId)
            .stream()
            .map(TablePart::getId)
            .map(tp -> new PartReplicas(tp.toString(), metaApi.replicas(tp).stream()
                .map(r -> new Replica(
                    r.getId().toString(),
                    r.getTable().toString(),
                    r.getPart().toString(),
                    r.getExecutor().toString(),
                    r.getHost(),
                    r.getPort()
                ))
                .collect(Collectors.toList())))
            .collect(Collectors.toList());
        return ResponseEntity.ok(parts);
    }

    @ApiOperation("Get table part stats")
    @GetMapping("/{table}/{part}/stats")
    public ResponseEntity<PartStats> getTablePartStats(@PathVariable Integer table, @PathVariable Integer part) {
        CommonId partId = new CommonId(ID_TYPE.stats, STATS_IDENTIFIER.part, PrimitiveCodec.encodeInt(table), part);
        TablePartStats stats = metaApi.tablePartStats(partId);
        return ResponseEntity.ok(new PartStats(
            stats.getId().toString(),
            stats.getTable().toString(),
            stats.getTablePart().toString(),
            stats.getLeader().toString(),
            stats.getApproximateStats().stream()
                .map(as -> new PartStats.ApproximateStats(
                    Arrays.toString(as.getStartKey()),
                    Arrays.toString(as.getEndKey()),
                    as.getCount(),
                    as.getSize()
                )).collect(Collectors.toList()),
            Collections.emptyList()
        ));
    }

    @ApiOperation("Get table part")
    @GetMapping("/{table}/{part}/part")
    public ResponseEntity<Part> getTablePart(@PathVariable Integer table, @PathVariable Integer part) {
        CommonId partId  = new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.part, PrimitiveCodec.encodeInt(table), part);
        TablePart tablePart = metaApi.tablePart(partId);
        return ResponseEntity.ok(new Part(
            tablePart.getId().toString(),
            tablePart.getTable().toString(),
            Arrays.toString(tablePart.getStart()),
            Arrays.toString(tablePart.getEnd())
        ));
    }

    @ApiOperation("Get table part")
    @GetMapping("/{table}/{part}/replica")
    public ResponseEntity<List<Replica>> getTablePartReplica(@PathVariable Integer table, @PathVariable Integer part) {
        CommonId partId = new CommonId(ID_TYPE.table, TABLE_IDENTIFIER.part, PrimitiveCodec.encodeInt(table), part);
        return ResponseEntity.ok(metaApi.replicas(partId).stream()
            .map(r -> new Replica(
                r.getId().toString(),
                r.getTable().toString(),
                r.getPart().toString(),
                r.getExecutor().toString(),
                r.getHost(),
                r.getPort()
            ))
            .collect(Collectors.toList())
        );
    }

}
