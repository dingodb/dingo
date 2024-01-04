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

package io.dingodb.client.operation.impl;

import io.dingodb.client.OperationContext;
import io.dingodb.client.common.Key;
import io.dingodb.client.common.TableInfo;
import io.dingodb.client.common.Value;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.Range;
import io.dingodb.sdk.common.RangeWithOptions;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.utils.Any;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.stream.Collectors;

import static io.dingodb.client.operation.RangeUtils.convert;
import static io.dingodb.client.operation.RangeUtils.getSubTasks;
import static io.dingodb.client.operation.RangeUtils.validateKeyRange;
import static io.dingodb.client.operation.RangeUtils.validateOpRange;

public class DeleteRangeOperation implements Operation {

    private static final DeleteRangeOperation INSTANCE = new DeleteRangeOperation(true);
    private static final DeleteRangeOperation NOT_STANDARD_INSTANCE = new DeleteRangeOperation(false);

    private DeleteRangeOperation(boolean standard) {
        this.standard = standard;

    }

    public static DeleteRangeOperation getInstance() {
        return INSTANCE;
    }

    public static DeleteRangeOperation getNotStandardInstance() {
        return NOT_STANDARD_INSTANCE;
    }

    private final boolean standard;

    @Override
    public Fork fork(Any parameters, TableInfo tableInfo) {
        try {
            KeyValueCodec codec = tableInfo.codec;
            NavigableSet<Task> subTasks = Collections.emptyNavigableSet();
            OpKeyRange keyRange = parameters.getValue();
            OpRange range;
            if (validateKeyRange(keyRange) && validateOpRange(range = convert(codec, tableInfo.definition, keyRange))) {
                subTasks = getSubTasks(tableInfo, range);
            }
            return new Fork(new DeleteRangeResult.DeleteResult[subTasks.size()], subTasks, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Fork fork(OperationContext context, TableInfo tableInfo) {
        /*OpRange range = context.parameters();
        NavigableSet<Task> subTasks = getSubTasks(tableInfo, range);
        return new Fork(context.result(), subTasks, true);*/
        return null;
    }

    @Override
    public void exec(OperationContext context) {
        OpRange range = context.parameters();
        long count;
        io.dingodb.client.common.KeyValueCodec codec = context.getCodec();
        try {
            DingoCommonId regionId = context.getRegionId();
            Range r = new Range(
                codec.resetPrefix(range.range.getStartKey(), regionId.parentId()),
                range.range.getEndKey());
            count = context.getStoreService().kvDeleteRange(
                context.getTableId(),
                regionId,
                new RangeWithOptions(r, range.withStart, range.withEnd));
        } catch (Exception e) {
            count = -1;
        }
        context.<DeleteRangeResult.DeleteResult[]>result()[context.getSeq()] = new DeleteRangeResult.DeleteResult(
            count,
            new OpKeyRange(
                new Key(Arrays.stream(
                    codec.decodeKeyPrefix(codec.resetPrefix(range.getStartKey(), context.getTableId().entityId()))
                ).map(Value::get).collect(Collectors.toList())),
                new Key(Arrays.stream(
                    codec.decodeKeyPrefix(codec.resetPrefix(range.getEndKey(), context.getTableId().entityId()))
                ).map(Value::get).collect(Collectors.toList())),
                range.withStart, range.withEnd)
        );
    }

    @Override
    public <R> R reduce(Fork context) {
        List<DeleteRangeResult.DeleteResult> resultList =
            Arrays.stream(context.<DeleteRangeResult.DeleteResult[]>result()).collect(Collectors.toList());
        long count = resultList.stream()
            .mapToLong(DeleteRangeResult.DeleteResult::getCount)
            .filter(__ -> __ > 0)
            .reduce(Long::sum).orElse(0L);
        return (R) new DeleteRangeResult(count, resultList);
    }

}
