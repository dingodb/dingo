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

package io.dingodb.exec.partition;

import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.NavigableMap;
@Slf4j
public class DingoPartitionStrategyFactory {
    public static final String RANGE_FUNC_NAME = "RANGE";
    public static final String HASH_FUNC_NAME = "HASH";
    public static final String UNSUPPORTED_PARTITION_FUNC_MSG = "Unsupported partition function: ";

    public static PartitionStrategy createPartitionStrategy(TableDefinition tableDefinition, NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions) {
        String funcName = tableDefinition.getPartDefinition() == null ? RANGE_FUNC_NAME : tableDefinition.getPartDefinition().getFuncName();
        log.trace("funcName:" + funcName);
        switch (funcName.toUpperCase()) {
            case RANGE_FUNC_NAME:
                return new RangeStrategy(tableDefinition, distributions);
            case HASH_FUNC_NAME:
                return new HashRangeStrategy(tableDefinition, distributions);
            default:
                throw new IllegalStateException(UNSUPPORTED_PARTITION_FUNC_MSG + funcName);
        }
    }
}
