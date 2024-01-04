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

package io.dingodb.client.common;

import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.type.converter.StrParseConverter;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.util.Optional;
import io.dingodb.sdk.common.partition.Partition;
import io.dingodb.sdk.common.partition.PartitionDetailDefinition;
import io.dingodb.sdk.common.partition.PartitionRule;
import io.dingodb.sdk.service.entity.common.IndexParameter;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IndexDefinition {

    private static final LongType LONG_TYPE = new LongType(false);

    private String name;
    private Integer version;
    private PartitionDefinition indexPartition;
    private Integer replica;
    private IndexParameter indexParameter;
    private Boolean isAutoIncrement;
    @Setter
    private Long autoIncrement;

}
