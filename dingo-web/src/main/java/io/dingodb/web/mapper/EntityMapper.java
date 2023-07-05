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

package io.dingodb.web.mapper;

import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.sdk.common.index.IndexDefinition;
import io.dingodb.sdk.common.partition.PartitionDetailDefinition;
import io.dingodb.sdk.common.partition.PartitionRule;
import io.dingodb.sdk.common.table.ColumnDefinition;
import io.dingodb.sdk.common.table.TableDefinition;
import io.dingodb.web.model.dto.VectorWithId;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;


@Mapper(unmappedSourcePolicy = ReportingPolicy.IGNORE, unmappedTargetPolicy = ReportingPolicy.IGNORE)
public interface EntityMapper {

    TableDefinition mapping(io.dingodb.common.table.TableDefinition definition);

    @Mapping(source = "typeName", target = "type")
    ColumnDefinition mapping(io.dingodb.common.table.ColumnDefinition definition);

    @Mapping(source = "indexParameter", target = "parameter")
    IndexDefinition mapping(io.dingodb.client.common.IndexDefinition definition);

    default PartitionRule mapping(PartitionDefinition definition) {
        return new PartitionRule(
            definition.getFuncName(),
            definition.getCols(),
            definition.getDetails().stream().map(this::mapping).collect(Collectors.toList()));
    }

    default PartitionDetailDefinition mapping(io.dingodb.common.partition.PartitionDetailDefinition definition) {
        return new PartitionDetailDefinition(definition.getPartName(), definition.getOperator(), definition.getOperand());
    }

    @Mapping(source = "metaData", target = "metaData")
    io.dingodb.client.common.VectorWithId mapping(VectorWithId withId);

    default Map<String, byte[]> mapping(Map<String, String> metaData) {
        return metaData.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getBytes(StandardCharsets.UTF_8)));
    }

    default VectorWithId mapping(io.dingodb.client.common.VectorWithId withId) {
        return new VectorWithId(withId.getId(), withId.getVector(), withId.getMetaData().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, __ -> Arrays.toString(__.getValue()))));
    }

    default Map<String, String> mapping(Properties properties) {
        Map<String, String> result = new HashMap<>();

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            result.put(key, value);
        }
        return result;
    }
}
