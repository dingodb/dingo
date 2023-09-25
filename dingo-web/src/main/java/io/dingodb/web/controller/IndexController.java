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
import io.dingodb.client.common.IndexDefinition;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.util.Optional;
import io.dingodb.sdk.common.index.Index;
import io.dingodb.sdk.common.index.IndexMetrics;
import io.dingodb.sdk.common.index.IndexParameter;
import io.dingodb.sdk.common.index.VectorIndexParameter;
import io.dingodb.web.Result;
import io.dingodb.web.mapper.EntityMapper;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@Api("Index")
@RestController
@RequestMapping("/index")
public class IndexController {

    @Autowired
    private DingoClient dingoClient;

    @Autowired
    private EntityMapper mapper;

    @ApiOperation("Create index")
    @PostMapping("/api/{schema}")
    public Result<Boolean> crateIndex(@PathVariable String schema, @RequestBody IndexDefinition definition) {
        try {
            if (!definition.getIsAutoIncrement()) {
                definition.setAutoIncrement(0L);
            }
            return Result.ok(dingoClient.createIndex(schema, definition.getName(), definition));
        } catch (Exception e) {
            return Result.errorMsg(e.getMessage());
        }
    }

    @ApiOperation("Update index")
    @PutMapping("/api/{schema}")
    public Result<Boolean> updateIndex(@PathVariable String schema, @RequestBody IndexDefinition definition) {
        try {
            return Result.ok(dingoClient.updateIndex(schema, definition.getName(), definition));
        } catch (Exception e) {
            return Result.errorMsg(e.getMessage());
        }
    }

    @ApiOperation("Update hnsw max_elements")
    @PutMapping("/api/{schema}/{index}/{maxElements}")
    public Result<Boolean> updateIndex(@PathVariable String schema, @PathVariable String index, @PathVariable Integer maxElements) {
        try {
            Index oldIndex = dingoClient.getIndex(schema, index);
            VectorIndexParameter vectorIndexParameter = oldIndex.getIndexParameter().getVectorIndexParameter();
            if (vectorIndexParameter.getHnswParam() == null) {
                return Result.errorMsg("Only max_elements in hnsw can be modified");
            }
            vectorIndexParameter.getHnswParam().setMaxElements(maxElements);
            IndexDefinition newIndex = IndexDefinition.builder()
                .name(index)
                .replica(oldIndex.getReplica())
                .version(oldIndex.getVersion())
                .isAutoIncrement(oldIndex.getIsAutoIncrement())
                .autoIncrement(oldIndex.getAutoIncrement())
                .indexPartition(Optional.mapOrGet(oldIndex.getIndexPartition(), __ -> new PartitionDefinition(
                    oldIndex.getIndexPartition().getFuncName(),
                    oldIndex.getIndexPartition().getCols(),
                    oldIndex.getIndexPartition().getDetails().stream()
                        .map(d -> new PartitionDetailDefinition(d.getPartName(), d.getOperator(), d.getOperand()))
                        .collect(Collectors.toList())), () -> null))
                .indexParameter(new IndexParameter(
                    oldIndex.getIndexParameter().getIndexType(),
                    new VectorIndexParameter(vectorIndexParameter.getVectorIndexType(), vectorIndexParameter.getHnswParam())))
                .build();

            return Result.ok(dingoClient.updateIndex(schema, index, newIndex));
        } catch (Exception e) {
            return Result.errorMsg(e.getMessage());
        }
    }

    @ApiOperation("Drop index")
    @DeleteMapping("/api/{schema}/{index}")
    public Result<Boolean> deleteIndex(@PathVariable String schema, @PathVariable String index) {
        try {
            return Result.ok(dingoClient.dropIndex(schema, index));
        } catch (Exception e) {
            return Result.errorMsg(e.getMessage());
        }
    }

    @ApiOperation("Get index")
    @GetMapping("/api/{schema}/{index}")
    public Result<Index> get(@PathVariable String schema, @PathVariable String index) {
        try {
            return Result.ok(dingoClient.getIndex(schema, index));
        } catch (Exception e) {
            return Result.errorMsg(e.getMessage());
        }
    }

    @ApiOperation("Get index metrics")
    @GetMapping("/api/{schema}/{index}/metrics")
    public Result<IndexMetrics> getIndexMetrics(@PathVariable String schema, @PathVariable String index) {
        try {
            return Result.ok(dingoClient.getIndexMetrics(schema, index));
        } catch (Exception e) {
            return Result.errorMsg(e.getMessage());
        }
    }

    @ApiOperation("Get index names")
    @GetMapping("/api/{schema}")
    public Result<List<String>> getNames(@PathVariable String schema) {
        try {
            return Result.ok(dingoClient.getIndexes(schema).stream().map(Index::getName).collect(Collectors.toList()));
        } catch (Exception e) {
            return Result.errorMsg(e.getMessage());
        }
    }
}
