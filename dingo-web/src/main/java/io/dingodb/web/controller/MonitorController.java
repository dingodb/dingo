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

import io.dingodb.web.model.vo.ClusterInfo;
import io.dingodb.web.model.vo.IndexInfo;
import io.dingodb.web.model.vo.Region;
import io.dingodb.web.model.vo.RegionDetailInfo;
import io.dingodb.web.model.vo.TableInfo;
import io.dingodb.web.model.vo.TreeSchema;
import io.dingodb.web.service.MonitorServerService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api("Monitor")
@RestController
@RequestMapping("monitor")
public class MonitorController {

    @Autowired
    private MonitorServerService monitorServerService;


    @ApiOperation("Get data")
    @GetMapping("/api/navigation")
    public ResponseEntity<List<TreeSchema>> getNavigation() {
        List<TreeSchema> schemas = monitorServerService.getNavigation("navigation");
        return ResponseEntity.ok(schemas);
    }

    @ApiOperation("Get region detail")
    @GetMapping("/api/queryRegion")
    public ResponseEntity<RegionDetailInfo> queryRegion(long regionId) {
        return ResponseEntity.ok(monitorServerService.getRegion(regionId));
    }

    @ApiOperation("Get table info")
    @GetMapping("/api/getTableInfo")
    public ResponseEntity<TableInfo> getTableInfo(String schema, String table) {
        String key = schema + "-" + table;
        TableInfo tableInfo = monitorServerService.getTableInfo(schema, table, key);
        return ResponseEntity.ok(tableInfo);
    }


    @ApiOperation("Get regions by partition ")
    @GetMapping("/api/getPartRegion")
    public ResponseEntity<List<Region>> queryRegionByPart(String schema, String table, Long partId) {
        String key = schema + "-" + table + "-" + partId;
        List<Region> regionList = monitorServerService.getRegionByPart(schema, table, partId, key);
        return ResponseEntity.ok(regionList);
    }

    @ApiOperation("Get regions by table")
    @GetMapping("/api/getTableRegion")
    public ResponseEntity<List<Region>> queryRegionByTable(String schema, String table) {
        String key = schema + "-" + table;
        List<Region> regionList = monitorServerService.getRegionByTable(schema, table, key);
        return ResponseEntity.ok(regionList);
    }

    @ApiOperation("Get index info")
    @GetMapping("/api/getIndexInfo")
    public ResponseEntity<IndexInfo> queryIndexInfo(String schema,
                                                    String table,
                                                    long indexId) {
        String key = schema + "-" + table + "-" + indexId;
        IndexInfo indexInfo = monitorServerService.getIndexInfo(schema, table, indexId, key);
        return ResponseEntity.ok(indexInfo);
    }

    @ApiOperation("Get regions by index")
    @GetMapping("/api/getIndexRegion")
    public ResponseEntity<List<Region>> queryRegionByIndex(String schema, String table, long indexId) {
        String key = schema + "-" + table + "-" + indexId;
        List<Region> regionList = monitorServerService.getRegionByIndex(schema, table, indexId, key);
        return ResponseEntity.ok(regionList);
    }

    @ApiOperation("Get cluster info")
    @GetMapping("/api/clusterStatus")
    public ResponseEntity<ClusterInfo> clusterInfo() {
        ClusterInfo clusterInfo = monitorServerService.getClusterResource("dingo");
        return ResponseEntity.ok(clusterInfo);
    }

}
