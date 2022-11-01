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

package io.dingodb.calcite.traits;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.plan.RelOptTable;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

@EqualsAndHashCode(callSuper = true, onlyExplicitlyIncluded = true)
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class DingoRelPartitionByTable extends DingoRelPartition {
    @Getter
    private final RelOptTable table;

    @EqualsAndHashCode.Include
    public @Nullable List<String> getTableName() {
        return table != null ? table.getQualifiedName() : null;
    }

    @Override
    public String toString() {
        return "TABLE(" + table.getQualifiedName() + ")";
    }
}
