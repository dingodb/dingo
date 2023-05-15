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
import io.dingodb.sdk.common.Range;
import io.dingodb.sdk.common.RangeWithOptions;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.common.utils.ByteArrayUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.dingodb.common.util.ByteArrayUtils.greatThan;
import static io.dingodb.sdk.common.utils.Any.wrap;
import static io.dingodb.sdk.common.utils.ByteArrayUtils.lessThan;

public class DeleteRangeOperation implements Operation {


    private static final DeleteRangeOperation INSTANCE = new DeleteRangeOperation();

    private DeleteRangeOperation() {
    }

    public static DeleteRangeOperation getInstance() {
        return INSTANCE;
    }

    private Comparator<Task> getComparator() {
        return (e1, e2) -> ByteArrayUtils.compare(e1.<OpRange>parameters().getStartKey(), e2.<OpRange>parameters().getStartKey());
    }

    @Override
    public Fork fork(Any parameters, Table table, RouteTable routeTable) {
        try {
            KeyValueCodec codec = routeTable.getCodec();
            OpKeyRange keyRange = parameters.getValue();
            List<Object> startKey = keyRange.start.getUserKey();
            List<Object> endKey = keyRange.end.getUserKey();
            OpRange range = new OpRange(
                codec.encodeKeyPrefix(startKey.toArray(new Object[table.getColumns().size()]), startKey.size()),
                codec.encodeKeyPrefix(endKey.toArray(new Object[table.getColumns().size()]), endKey.size()),
                keyRange.withStart,
                keyRange.withEnd
            );
            if (greatThan(range.getStartKey(), range.getEndKey())
                || (Arrays.equals(range.getStartKey(), range.getEndKey())) && (!range.withEnd || !range.withStart)) {
                return new Fork(new Iterator[0], Collections.emptyNavigableSet(), true);
            }
            NavigableSet<Task> subTasks = getSubTasks(routeTable, range);
            Task task = subTasks.pollFirst();
            if (task == null) {
                return new Fork(new long[0], subTasks, true);
            }
            buildSubTasks(range, subTasks, task);
            return new Fork(new long[subTasks.size()], subTasks, true);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Fork fork(OperationContext context, RouteTable routeTable) {
        OpRange range = context.parameters();
        NavigableSet<Task> subTasks = getSubTasks(routeTable, range);
        Task task = subTasks.pollFirst();
        if (task == null) {
            return new Fork(new long[0], subTasks, true);
        }
        buildSubTasks(range, subTasks, task);
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

    private NavigableSet<Task> getSubTasks(RouteTable routeTable, OpRange range) {
        NavigableSet<Task> subTasks = new TreeSet<>(getComparator());
        Predicate<byte[]> filter = (k) -> ByteArrayUtils.greatThan(range.getEndKey() , k) || (ByteArrayUtils.compareWithoutLen(range.getEndKey(), k) == 0 && range.withEnd);
        Function<Range, byte[]> keyGetter = Range::getStartKey;
        for (RangeDistribution rd : routeTable.getRangeDistribution().descendingMap().values()) {
            if (filter.test(keyGetter.apply(rd.getRange()))) {
                if (subTasks.isEmpty()) {
                    filter = k -> lessThan(range.getStartKey(), k);
                    keyGetter = Range::getEndKey;
                }
                subTasks.add(new Task(
                    rd.getId(),
                    wrap(new OpRange(rd.getRange().getStartKey(), rd.getRange().getEndKey(), true, false))
                ));
            }
        }
        return subTasks;
    }

    private void buildSubTasks(OpRange range, NavigableSet<Task> subTasks, Task task) {
        OpRange taskRange = task.parameters();
        subTasks.add(new Task(
                task.getRegionId(),
                wrap(new OpRange(range.getStartKey(), taskRange.getEndKey(), range.withStart, taskRange.withEnd))
        ));
        task = subTasks.pollLast();
        taskRange = task.parameters();
        subTasks.add(new Task(
                task.getRegionId(),
                wrap(new OpRange(taskRange.getStartKey(), range.getEndKey(), taskRange.withStart, range.withEnd))
        ));
    }
}
