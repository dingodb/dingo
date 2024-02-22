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

package io.dingodb.exec.operator;

import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.CompareAndSetParam;
import io.dingodb.store.api.StoreInstance;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;

public class CompareAndSetOperator extends PartModifyOperator {
    public static final CompareAndSetOperator INSTANCE = new CompareAndSetOperator();

    private CompareAndSetOperator() {
    }

    @Override
    protected boolean pushTuple(Context context, @Nullable Object[] tuple, Vertex vertex) {
        CompareAndSetParam param = vertex.getParam();
        DingoType schema = param.getSchema();

        RangeDistribution distribution = context.getDistribution();
        int tupleSize = schema.fieldCount();
        Object[] oldTuple = Arrays.copyOf(tuple, tupleSize);
        Object[] newTuple = Arrays.copyOfRange(tuple, oldTuple.length, tuple.length);
        if (oldTuple.length != newTuple.length) {
            throw new RuntimeException("Compare and set Operator Exception");
        }

        StoreInstance store = Services.KV_STORE.getInstance(param.getTableId(), distribution.getId());
        KeyValue old = param.getCodec().encode(oldTuple);
        KeyValue row = param.getCodec().encode(newTuple);
        if (store.update(row, old)) {
            param.inc();
            context.addKeyState(true);
        } else {
            context.addKeyState(false);
        }

        return true;
    }
}
