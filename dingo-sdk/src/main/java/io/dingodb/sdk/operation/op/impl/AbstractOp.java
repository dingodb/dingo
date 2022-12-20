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

package io.dingodb.sdk.operation.op.impl;

import io.dingodb.common.CommonId;
import io.dingodb.sdk.common.Key;
import io.dingodb.sdk.common.Record;
import io.dingodb.sdk.operation.Column;
import io.dingodb.sdk.operation.context.Context;
import io.dingodb.sdk.operation.executive.UpdateExec;
import io.dingodb.sdk.operation.executive.collection.GetExec;
import io.dingodb.sdk.operation.executive.collection.ScanExec;
import io.dingodb.sdk.operation.executive.write.DeleteExec;
import io.dingodb.sdk.operation.executive.write.DeleteRangeExec;
import io.dingodb.sdk.operation.executive.write.PutExec;
import io.dingodb.sdk.operation.op.Op;

import java.util.Collections;
import java.util.List;

public abstract class AbstractOp implements Op {

    public transient Op head;
    public Op next;
    public int ident = 0; // 0: read 1: write

    private final CommonId execId;
    private final Context context;

    public AbstractOp(CommonId execId, Context context) {
        this(execId, context, null, 0);
    }

    public AbstractOp(CommonId execId, Context context, Op head) {
        this(execId, context, head, 0);
    }

    public AbstractOp(CommonId execId, Context context, Op head, int ident) {
        this.execId = execId;
        this.context = context;
        this.head = head;
        this.ident = ident;
    }

    @Override
    public Op next() {
        return next;
    }

    @Override
    public Op head() {
        return head;
    }

    @Override
    public CommonId execId() {
        return execId;
    }

    @Override
    public Context context() {
        return context;
    }

    public static CollectionOp scan(Key start, Key end) {
        return new CollectionOp(
            ScanExec.COMMON_ID, Context.builder()
            .startPrimaryKeys(Collections.singletonList(start))
            .endPrimaryKeys(Collections.singletonList(end))
            .build());
    }

    public static CollectionOp get(List<Key> keyList) {
        return new CollectionOp(GetExec.COMMON_ID, Context.builder().startPrimaryKeys(keyList).build());
    }

    public static WriteOp put(List<Key> keyList, List<Record> recordList, boolean skippedWhenExisted) {
        return new WriteOp(PutExec.COMMON_ID, Context.builder()
            .startPrimaryKeys(keyList)
            .recordList(recordList)
            .skippedWhenExisted(skippedWhenExisted)
            .build());
    }

    public static WriteOp update(List<Key> keyList, Column[] columns, boolean useDefaultWhenNotExisted) {
        return new WriteOp(UpdateExec.COMMON_ID, Context.builder()
            .startPrimaryKeys(keyList)
            .column(columns)
            .useDefaultWhenNotExisted(useDefaultWhenNotExisted)
            .build());
    }

    public static WriteOp delete(List<Key> keyList) {
        return new WriteOp(DeleteExec.COMMON_ID, Context.builder().startPrimaryKeys(keyList).build());
    }

    public static WriteOp deleteRange(List<Key> startKeyList, List<Key> endKeyList) {
        return new WriteOp(DeleteRangeExec.COMMON_ID, Context.builder()
            .startPrimaryKeys(startKeyList)
            .endPrimaryKeys(endKeyList)
            .build());
    }
}
