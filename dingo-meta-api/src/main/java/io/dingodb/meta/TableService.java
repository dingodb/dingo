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

package io.dingodb.meta;

import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;

public interface TableService {

    default void createIndex(CommonId tableId, TableDefinition table, TableDefinition index) {
        throw new UnsupportedOperationException();
    }

    default void dropIndex(CommonId tableId, CommonId indexId) {
        throw new UnsupportedOperationException();
    }

    Map<CommonId, TableDefinition> getTableIndexDefinitions(@NonNull CommonId id);

    TableStatistic getTableStatistic(@NonNull String tableName);

    Long getAutoIncrement(CommonId tableId);

    Long getNextAutoIncrement(CommonId tableId);

    default NavigableSet<RangeDistribution> getRangeDistributions(CommonId id) {
        throw new UnsupportedOperationException();
    }

    default NavigableSet<RangeDistribution> getHashRangeDistributions(CommonId id) {
        return null;
    }

    /**
     * Get range distributions by table id.
     *
     * @param id table id
     * @return table range distributions
     */
    default NavigableMap<ComparableByteArray, RangeDistribution> getRangeDistribution(CommonId id) {
        // todo
        throw new UnsupportedOperationException();
    }



    default NavigableMap<ComparableByteArray, RangeDistribution> getIndexRangeDistribution(
        CommonId id, TableDefinition tableDefinition
    ) {
        throw new UnsupportedOperationException();
    }

    default NavigableMap<ComparableByteArray, RangeDistribution> getIndexRangeDistribution(@NonNull CommonId id) {
        throw new UnsupportedOperationException();
    }

    default void addDistribution(CommonId distributionId, RangeDistribution distribution) {
        throw new UnsupportedOperationException();
    }

}
