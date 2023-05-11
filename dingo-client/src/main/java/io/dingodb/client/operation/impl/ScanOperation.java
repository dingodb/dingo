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
import io.dingodb.client.common.Record;
import io.dingodb.client.common.RouteTable;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.Range;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.common.utils.ByteArrayUtils;
import io.dingodb.sdk.common.utils.LinkedIterator;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static io.dingodb.common.util.ByteArrayUtils.compareWithoutLen;
import static io.dingodb.sdk.common.utils.Any.wrap;

public class ScanOperation implements Operation {

    private static final ScanOperation INSTANCE = new ScanOperation();

    private ScanOperation() {
    }

    public static ScanOperation getInstance() {
        return INSTANCE;
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
            NavigableSet<Task> subTasks = getSubTasks(routeTable, range);
            Task task = subTasks.pollFirst();
            if (task == null) {
                return new Fork(new Iterator[0], subTasks, true);
            }
            buildSubTasks(range, subTasks, task);
            return new Fork(new Iterator[subTasks.size()], subTasks, true);
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
            return new Fork(new Iterator[0], subTasks, true);
        }
        buildSubTasks(range, subTasks, task);
        return new Fork(context.result(), subTasks, true);
    }

    private Comparator<Task> getComparator() {
        return (e1, e2) -> ByteArrayUtils.compare(e1.<OpRange>parameters().getStartKey(), e2.<OpRange>parameters().getStartKey());
    }

    @Override
    public void exec(OperationContext context) {
        OpRange scan = context.parameters();

        Iterator<KeyValue> scanResult = context.getStoreService()
            .scan(context.getTableId(), context.getRegionId(), scan.range, scan.withStart, scan.withEnd);

        context.<Iterator<Record>[]>result()[context.getSeq()] = new RecordIterator(
            context.getTable().getColumns(), context.getCodec(), scanResult
        );
    }

    @Override
    public <R> R reduce(Fork fork) {
        LinkedIterator<Record> result = new LinkedIterator<>();
        Arrays.stream(fork.<Iterator<Record>[]>result()).forEach(result::append);
        return (R) result;
    }

    private boolean isRequireRange(Range range, OpRange scanRange) {
        byte[] rangeStart = range.getStartKey();
        byte[] rangeEnd = range.getEndKey();
        byte[] scanStart = scanRange.getStartKey();
        byte[] scanEnd = scanRange.getEndKey();
        int startExpect = scanRange.isWithStart() ? 0 : 1;
        int endExpect = scanRange.isWithEnd() ? 0 : -1;
        return (compareWithoutLen(rangeStart, scanStart) >= startExpect && compareWithoutLen(rangeStart, scanEnd) <= endExpect)
            || (compareWithoutLen(rangeEnd, scanStart) >= startExpect && compareWithoutLen(rangeEnd, scanEnd) <= endExpect);
    }

    private NavigableSet<Task> getSubTasks(RouteTable routeTable, OpRange range) {
        NavigableSet<Task> subTasks = new TreeSet<>(getComparator());
        routeTable.getRangeDistribution().values().stream()
            .filter(rangeDistribution -> isRequireRange(rangeDistribution.getRange(), range))
            .map(rd -> new Task(
                rd.getId(), wrap(
                new OpRange(rd.getRange().getStartKey(), rd.getRange().getEndKey(), true, false)))
            ).forEach(subTasks::add);
        return subTasks;
    }

    private void buildSubTasks(OpRange range, NavigableSet<Task> subTasks, Task task) {
        OpRange taskScan = task.parameters();
        subTasks.add(new Task(
                task.getRegionId(),
                wrap(new OpRange(range.getStartKey(), taskScan.getEndKey(), range.withStart, taskScan.withEnd))
        ));
        task = subTasks.pollLast();
        taskScan = task.parameters();
        subTasks.add(new Task(
                task.getRegionId(),
                wrap(new OpRange(taskScan.getStartKey(), range.getEndKey(), taskScan.withStart, range.withEnd))
        ));
    }
}
