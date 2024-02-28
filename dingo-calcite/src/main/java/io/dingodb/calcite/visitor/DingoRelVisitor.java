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
import io.dingodb.calcite.rel.DingoExportData;
import io.dingodb.calcite.rel.DingoFilter;
import io.dingodb.calcite.rel.DingoFunctionScan;
import io.dingodb.calcite.rel.DingoGetByIndex;
import io.dingodb.calcite.rel.DingoGetByIndexMerge;
import io.dingodb.calcite.rel.DingoGetByKeys;
import io.dingodb.calcite.rel.DingoGetVectorByDistance;
import io.dingodb.calcite.rel.dingo.DingoHashJoin;
import io.dingodb.calcite.rel.DingoInfoSchemaScan;
import io.dingodb.calcite.rel.DingoLikeScan;
import io.dingodb.calcite.rel.DingoPartCountDelete;
import io.dingodb.calcite.rel.DingoPartRangeDelete;
import io.dingodb.calcite.rel.DingoProject;
import io.dingodb.calcite.rel.DingoReduce;
import io.dingodb.calcite.rel.dingo.DingoRoot;
import io.dingodb.calcite.rel.dingo.DingoSort;
import io.dingodb.calcite.rel.dingo.DingoStreamingConverter;
import io.dingodb.calcite.rel.DingoTableModify;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.rel.DingoUnion;
import io.dingodb.calcite.rel.DingoValues;
import io.dingodb.calcite.rel.DingoVector;
import io.dingodb.calcite.rel.VectorStreamConvertor;
import io.dingodb.calcite.rel.dingo.DingoReduceAggregate;
import io.dingodb.calcite.rel.dingo.DingoRelOp;
import io.dingodb.calcite.rel.dingo.DingoScanWithRelOp;
import org.checkerframework.checker.nullness.qual.NonNull;

public interface DingoRelVisitor<T> {
    T visit(@NonNull DingoAggregate rel);

    T visit(@NonNull DingoFilter rel);

    T visit(@NonNull DingoGetByIndex rel);

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

    T visit(@NonNull DingoPartRangeDelete rel);

    T visit(@NonNull DingoLikeScan rel);

    T visit(@NonNull DingoFunctionScan rel);

    T visit(@NonNull DingoVector rel);

    T visit(@NonNull DingoGetVectorByDistance rel);

    T visit(@NonNull VectorStreamConvertor rel);

    T visit(@NonNull DingoGetByIndexMerge rel);

    T visit(@NonNull DingoInfoSchemaScan rel);

    T visit(@NonNull DingoExportData rel);

    T visitDingoRelOp(@NonNull DingoRelOp rel);

    T visitDingoScanWithRelOp(@NonNull DingoScanWithRelOp rel);

    T visitDingoAggregateReduce(@NonNull DingoReduceAggregate rel);
}
