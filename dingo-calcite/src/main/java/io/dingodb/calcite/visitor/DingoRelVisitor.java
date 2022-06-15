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

package io.dingodb.calcite.visitor;

import io.dingodb.calcite.rel.DingoAggregate;
import io.dingodb.calcite.rel.DingoCoalesce;
import io.dingodb.calcite.rel.DingoDistributedValues;
import io.dingodb.calcite.rel.DingoExchange;
import io.dingodb.calcite.rel.DingoExchangeRoot;
import io.dingodb.calcite.rel.DingoFilter;
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.DingoHash;
import io.dingodb.calcite.rel.DingoHashJoin;
import io.dingodb.calcite.rel.DingoPartCountDelete;
import io.dingodb.calcite.rel.DingoPartModify;
import io.dingodb.calcite.rel.DingoPartScan;
import io.dingodb.calcite.rel.DingoPartition;
import io.dingodb.calcite.rel.DingoProject;
import io.dingodb.calcite.rel.DingoReduce;
import io.dingodb.calcite.rel.DingoSort;
import io.dingodb.calcite.rel.DingoTableModify;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.rel.DingoUnion;
import io.dingodb.calcite.rel.DingoValues;

import javax.annotation.Nonnull;

public interface DingoRelVisitor<T> {
    T visit(@Nonnull DingoAggregate rel);

    T visit(@Nonnull DingoCoalesce rel);

    T visit(@Nonnull DingoDistributedValues rel);

    T visit(@Nonnull DingoExchange rel);

    T visit(@Nonnull DingoExchangeRoot rel);

    T visit(@Nonnull DingoFilter rel);

    T visit(@Nonnull DingoGetByKeys rel);

    T visit(@Nonnull DingoHash rel);

    T visit(@Nonnull DingoHashJoin rel);

    T visit(@Nonnull DingoPartition rel);

    T visit(@Nonnull DingoPartModify rel);

    T visit(@Nonnull DingoPartScan rel);

    T visit(@Nonnull DingoProject rel);

    T visit(@Nonnull DingoReduce rel);

    T visit(@Nonnull DingoSort rel);

    T visit(@Nonnull DingoTableModify rel);

    T visit(@Nonnull DingoTableScan rel);

    T visit(@Nonnull DingoUnion rel);

    T visit(@Nonnull DingoValues rel);

    T visit(@Nonnull DingoPartCountDelete rel);
}
