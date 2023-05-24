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

import io.dingodb.sdk.common.partition.PartitionDetail;

public class PartitionDetailDefinition implements PartitionDetail {

    private final io.dingodb.common.partition.PartitionDetailDefinition partitionDetailDefinition;

    public PartitionDetailDefinition(io.dingodb.common.partition.PartitionDetailDefinition partitionDetailDefinition) {
        this.partitionDetailDefinition = partitionDetailDefinition;
    }

    @Override
    public String getPartName() {
        return partitionDetailDefinition.getPartName();
    }

    @Override
    public String getOperator() {
        return partitionDetailDefinition.getOperator();
    }

    @Override
    public Object[] getOperand() {
        return partitionDetailDefinition.getOperand();
    }
}
