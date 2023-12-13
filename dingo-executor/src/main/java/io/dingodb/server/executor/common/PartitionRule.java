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

package io.dingodb.server.executor.common;

import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.sdk.common.partition.Partition;
import io.dingodb.sdk.common.partition.PartitionDetail;

import java.util.List;
import java.util.stream.Collectors;

public class PartitionRule implements Partition {

    private final PartitionDefinition partitionDefinition;

    public PartitionRule(PartitionDefinition partitionDefinition) {
        this.partitionDefinition = partitionDefinition;
    }

    @Override
    public String getFuncName() {
        return partitionDefinition.getFuncName();
    }

    @Override
    public List<String> getCols() {
        return partitionDefinition.getColumns();
    }

    @Override
    public List<PartitionDetail> getDetails() {
        return partitionDefinition.getDetails()
            .stream()
            .map(PartitionDetailDefinition::new)
            .collect(Collectors.toList());
    }
}
