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
import io.dingodb.calcite.rel.DingoFilter;
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.DingoHashJoin;
import io.dingodb.calcite.rel.DingoPartCountDelete;
import io.dingodb.calcite.rel.DingoPartRangeDelete;
import io.dingodb.calcite.rel.DingoPartRangeScan;
import io.dingodb.calcite.rel.DingoProject;
import io.dingodb.calcite.rel.DingoReduce;
import io.dingodb.calcite.rel.DingoRoot;
import io.dingodb.calcite.rel.DingoSort;
import io.dingodb.calcite.rel.DingoStreamingConverter;
import io.dingodb.calcite.rel.DingoTableModify;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.rel.DingoUnion;
import io.dingodb.calcite.rel.DingoValues;
import org.checkerframework.checker.nullness.qual.NonNull;

public interface DingoRelVisitor<T> {
    T visit(@NonNull DingoAggregate rel);

    T visit(@NonNull DingoFilter rel);

    T visit(@NonNull DingoGetByKeys rel);

    T visit(@NonNull DingoHashJoin rel);

    T visit(@NonNull DingoTableModify rel);

    T visit(@NonNull DingoProject rel);

    T visit(@NonNull DingoReduce rel);

    T visit(@NonNull DingoRoot rel);

    T visit(@NonNull DingoSort rel);

    T visit(@NonNull DingoStreamingConverter rel);

    T visit(@NonNull DingoTableScan rel);

    T visit(@NonNull DingoUnion rel);

    T visit(@NonNull DingoValues rel);

    T visit(@NonNull DingoPartCountDelete rel);

    T visit(@NonNull DingoPartRangeScan rel);

    T visit(@NonNull DingoPartRangeDelete rel);
}
