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
import io.dingodb.client.common.RouteTable;
import io.dingodb.sdk.common.RangeWithOptions;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.Any;

import java.util.Arrays;
import java.util.Collections;
import java.util.NavigableSet;

import static io.dingodb.client.operation.RangeUtils.convert;
import static io.dingodb.client.operation.RangeUtils.getSubTasks;
import static io.dingodb.client.operation.RangeUtils.validateOpRange;
import static io.dingodb.client.operation.RangeUtils.validateKeyRange;

public class DeleteRangeOperation implements Operation {


    private static final DeleteRangeOperation INSTANCE = new DeleteRangeOperation();

    private DeleteRangeOperation() {
    }

    public static DeleteRangeOperation getInstance() {
        return INSTANCE;
    }

    @Override
    public Fork fork(Any parameters, Table table, RouteTable routeTable) {
        try {
            KeyValueCodec codec = routeTable.codec;
            NavigableSet<Task> subTasks = Collections.emptyNavigableSet();
            OpKeyRange keyRange = parameters.getValue();
            OpRange range;
            if (validateKeyRange(keyRange) && validateOpRange(range = convert(codec, table, keyRange))) {
                subTasks = getSubTasks(routeTable, range);
            }
            return new Fork(new long[subTasks.size()], subTasks, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Fork fork(OperationContext context, RouteTable routeTable) {
        OpRange range = context.parameters();
        NavigableSet<Task> subTasks = getSubTasks(routeTable, range);
        return new Fork(context.result(), subTasks, true);
    }

    @Override
    public void exec(OperationContext context) {
        OpRange range = context.parameters();
        context.<long[]>result()[context.getSeq()] = context.getStoreService()
            .kvDeleteRange(context.getTableId(), context.getRegionId(), new RangeWithOptions(range.range, range.withStart, range.withEnd));
    }

    @Override
    public <R> R reduce(Fork context) {
        return (R) (Long) Arrays.stream(context.<long[]>result()).reduce(Long::sum).orElse(0L);
    }

}
