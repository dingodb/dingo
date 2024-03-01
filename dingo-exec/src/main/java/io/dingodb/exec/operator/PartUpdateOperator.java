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
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.Services;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.PartUpdateParam;
import io.dingodb.meta.MetaService;
import io.dingodb.store.api.StoreInstance;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;

@Slf4j
public final class PartUpdateOperator extends PartModifyOperator {
    public static final PartUpdateOperator INSTANCE = new PartUpdateOperator();

    private PartUpdateOperator() {
    }

    @Override
    public boolean pushTuple(Context context, Object[] tuple, Vertex vertex) {
        PartUpdateParam param = vertex.getParam();
        RangeDistribution distribution = context.getDistribution();
        DingoType schema = param.getSchema();
        TupleMapping mapping = param.getMapping();
        List<SqlExpr> updates = param.getUpdates();
        // The input tuple contains all old values and the new values, so make a new tuple for updating.
        // The new values are not converted to correct type, so are useless.
        int tupleSize = schema.fieldCount();
        Object[] newTuple = Arrays.copyOf(tuple, tupleSize);
        boolean updated = false;
        int i = 0;
        try {
            for (i = 0; i < mapping.size(); ++i) {
                // This is the new value.
                Object newValue = updates.get(i).eval(tuple);
                int index = mapping.get(i);
                if ((newTuple[index] == null && newValue != null)
                    || (newTuple[index] != null && !newTuple[index].equals(newValue))
                ) {
                    newTuple[index] = newValue;
                    updated = true;
                }
            }
            if (param.isHasAutoInc() && param.getAutoIncColIdx() < tuple.length) {
                long autoIncVal = Long.parseLong(newTuple[param.getAutoIncColIdx()].toString());
                MetaService metaService = MetaService.root();
                metaService.updateAutoIncrement(param.getTableId(), autoIncVal);
            }
            Object[] newTuple2 = (Object[]) schema.convertFrom(newTuple, ValueConverter.INSTANCE);
            Object[] oldTuple = Arrays.copyOf(tuple, tupleSize);
            StoreInstance store = Services.KV_STORE.getInstance(param.getTableId(), distribution.getId());
            if (store.insertIndex(newTuple2)) {
                if (store.updateWithIndex(newTuple2, oldTuple)) {
                    store.deleteIndex(newTuple2, oldTuple);
                }
            }
            if (updated) {
                param.inc();
                context.addKeyState(true);
            } else {
                context.addKeyState(false);
            }
        } catch (Exception ex) {
            log.error("update operator with exprs: {}", updates, ex);
            throw new RuntimeException("Update Operator catch Exception");
        }
        return true;
    }

}
