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
import io.dingodb.sdk.operation.context.Context;
import io.dingodb.sdk.operation.executive.collection.GetExec;
import io.dingodb.sdk.operation.executive.collection.ScanExec;
import io.dingodb.sdk.operation.executive.write.DeleteExec;
import io.dingodb.sdk.operation.executive.write.DeleteRangeExec;
import io.dingodb.sdk.operation.executive.write.PutExec;
import io.dingodb.sdk.operation.op.Op;

import java.util.Collections;
import java.util.List;

public abstract class AbstractOp implements Op {

    public static Op head;
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
        head = new CollectionOp(
            ScanExec.COMMON_ID, new Context(Collections.singletonList(start), Collections.singletonList(end)));
        return (CollectionOp) head;
    }

    public static CollectionOp get(List<Key> primaryKeys) {
        head = new CollectionOp(GetExec.COMMON_ID, new Context(primaryKeys, null));
        return (CollectionOp) head;
    }

    public static WriteOp put(List<Key> keyList, List<Record> recordList, boolean skippedWhenExisted) {
        head = new WriteOp(PutExec.COMMON_ID, new Context(keyList, recordList, skippedWhenExisted));
        return (WriteOp) head;
    }

    public static WriteOp delete(List<Key> keyList) {
        head = new WriteOp(DeleteExec.COMMON_ID, new Context(keyList, null));
        return (WriteOp) head;
    }

    public static WriteOp deleteRange(List<Key> starts, List<Key> ends) {
        head = new WriteOp(DeleteRangeExec.COMMON_ID, new Context(starts, ends), null);
        return (WriteOp) head;
    }
}
