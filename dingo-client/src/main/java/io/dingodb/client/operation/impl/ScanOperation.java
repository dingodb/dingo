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
import io.dingodb.client.common.TableInfo;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.Range;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.common.utils.LinkedIterator;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NavigableSet;

import static io.dingodb.client.operation.RangeUtils.convert;
import static io.dingodb.client.operation.RangeUtils.getSubTasks;
import static io.dingodb.client.operation.RangeUtils.validateKeyRange;
import static io.dingodb.client.operation.RangeUtils.validateOpRange;

public class ScanOperation implements Operation {

    private static final ScanOperation INSTANCE = new ScanOperation(true);
    private static final ScanOperation NOT_STANDARD_INSTANCE = new ScanOperation(false);

    private ScanOperation(boolean standard) {
        this.standard = standard;
    }

    public static ScanOperation getInstance() {
        return INSTANCE;
    }

    public static ScanOperation getNotStandardInstance() {
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
            return new Fork(new Iterator[subTasks.size()], subTasks, false);
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
        OpRange scan = context.parameters();

        Range range = scan.range;
        Iterator<KeyValue> scanResult = context.getStoreService().scan(
            context.getTableId(),
            context.getRegionId(),
            new Range(
                context.getCodec().resetPrefix(range.getStartKey(), context.getRegionId().parentId()),
                range.getEndKey()),
            scan.withStart,
            scan.withEnd);

        context.<Iterator<Record>[]>result()[context.getSeq()] = new RecordIterator(
            context.getTable().getColumns(),
            standard ? context.getCodec() : context.getCodec().getKeyValueCodec(),
            scanResult,
            context.getTableId().entityId()
        );
    }

    @Override
    public <R> R reduce(Fork fork) {
        LinkedIterator<Record> result = new LinkedIterator<>();
        Arrays.stream(fork.<Iterator<Record>[]>result()).forEach(result::append);
        return (R) result;
    }

}
