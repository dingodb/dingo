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

package io.dingodb.sdk.operation.op;

import io.dingodb.common.CommonId;
import io.dingodb.sdk.common.Key;
import io.dingodb.sdk.common.Record;
import io.dingodb.sdk.operation.Column;
import io.dingodb.sdk.operation.context.Context;
import io.dingodb.sdk.operation.op.impl.AbstractOp;
import io.dingodb.sdk.operation.op.impl.CollectionOp;
import io.dingodb.sdk.operation.op.impl.WriteOp;

import java.util.Collections;
import java.util.List;

public interface Op {

    static CollectionOp scan(Key start, Key end) {
        return AbstractOp.scan(start, end);
    }

    static CollectionOp get(Key primaryKey) {
        return get(Collections.singletonList(primaryKey));
    }

    static CollectionOp get(List<Key> primaryKeys) {
        return AbstractOp.get(primaryKeys);
    }

    static WriteOp put(List<Key> keyList, List<Record> recordList, boolean skippedWhenExisted) {
        return AbstractOp.put(keyList, recordList, skippedWhenExisted);
    }

    static WriteOp update(Key key, Column[] columns, boolean useDefaultWhenNotExisted) {
        return update(Collections.singletonList(key), columns, useDefaultWhenNotExisted);
    }

    static WriteOp update(List<Key> keyList, Column[] columns, boolean useDefaultWhenNotExisted) {
        return AbstractOp.update(keyList, columns, useDefaultWhenNotExisted);
    }

    static WriteOp delete(List<Key> keyList) {
        return AbstractOp.delete(keyList);
    }

    static WriteOp deleteRange(Key start, Key end) {
        return AbstractOp.deleteRange(start, end);
    }

    Op next();

    Op head();

    boolean readOnly();

    void readOnly(boolean readOnly);

    Context context();

    CommonId execId();
}
