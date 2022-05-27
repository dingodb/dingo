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

import io.dingodb.common.Location;
import io.dingodb.meta.Part;
import io.dingodb.web.model.PartReplicaRes;
import io.dingodb.web.model.PartRequest;
import io.dingodb.web.model.PartTableRes;
import io.dingodb.web.service.MetricsService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api("Part")
@RestController
@RequestMapping("/part")
public class MetricsController {

    @Autowired
    private MetricsService metricsService;

    @ApiOperation("Get table")
    @PostMapping("/table")
    public ResponseEntity<PartTableRes> getTablePart(@RequestBody PartRequest req) {
        List<Location> locations = metricsService.partTable(req.getSchema(), req.getName());
        return ResponseEntity.ok(new PartTableRes(req.getSchema(), req.getName(), locations));
    }

    @ApiOperation("Get table replica")
    @PostMapping("/replica")
    public ResponseEntity<PartReplicaRes> getPartReplica(@RequestBody PartRequest req) {
        Part part = metricsService.partReplica(req.getSchema(), req.getName());
        return ResponseEntity.ok(
            new PartReplicaRes(req.getSchema(), req.getName(), part.getLeader(), part.getReplicates()));
    }
}
