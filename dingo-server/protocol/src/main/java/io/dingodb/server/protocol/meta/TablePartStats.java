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

package io.dingodb.server.protocol.meta;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.table.TableDefinition;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.FieldNameConstants;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@Setter
@Builder
@ToString
@FieldNameConstants(asEnum = true)
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class TablePartStats implements Stats {

    public static final TableDefinition DEFINITION = new MetaTableDefinitionBuilder("TABLE_PART_STATS")
        .addColumns(Arrays.stream(Fields.values()).map(Enum::name).collect(Collectors.toList()))
        .build();

    private CommonId id;
    private long time;

    private CommonId table;
    private CommonId tablePart;
    private CommonId leader;
    private long writePerSecond;
    private long readPerSecond;
    private long writeBytesPerSecond;
    private long readBytesPerSecond;
    private List<ApproximateStats> approximateStats;
    private List<Location> alive;

    @Getter
    @Setter
    @ToString
    @AllArgsConstructor
    public static class ApproximateStats {
        byte[] startKey;
        byte[] endKey;
        Long count;
        Long size;
    }

}
