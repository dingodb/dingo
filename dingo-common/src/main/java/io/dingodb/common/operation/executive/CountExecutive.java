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

package io.dingodb.common.operation.executive;

import io.dingodb.common.operation.DingoExecResult;
import io.dingodb.common.operation.Value;
import io.dingodb.common.operation.compute.NumericType;
import io.dingodb.common.operation.context.BasicContext;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.TableDefinition;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Slf4j
public class CountExecutive extends NumberExecutive<BasicContext, Iterator<KeyValue>, Object> {

    @Override
    public DingoExecResult execute(BasicContext context, Iterator<KeyValue> records) {
        int[] keyIndex = getKeyIndex(context);
        int[] valueIndex = getValueIndex(context);
        TableDefinition definition = context.definition;
        Map<String, Value> result = new HashMap<>();

        while (records.hasNext()) {
            KeyValue keyValue = records.next();
            boolean flag;
            if (context.filter == null) {
                flag = true;
            } else {
                flag = context.filter.filter(context, keyValue);
            }
            if (flag) {
                try {
                    if (valueIndex.length > 0) {
                        Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), valueIndex);
                        for (int i = 0; i < objects.length; i++) {
                            Object obj = objects[i];
                            if (obj != null) {
                                String name = definition.getColumn(
                                    valueIndex[i] + definition.getPrimaryKeyCount()).getName();
                                Value v = result.get(name);
                                v = v == null ? Value.get(1) : Value.get(v.value().intValue() + 1);
                                result.put(name, v);
                            }
                        }
                    }
                    if (keyIndex.length > 0) {
                        Object[] objects = context.dingoKeyCodec().decode(keyValue.getKey(), valueIndex);
                        for (int i = 0; i < objects.length; i++) {
                            Value v = result.get(definition.getColumn(keyIndex[i]).getName());
                            v = v == null ? Value.get(1) : Value.get(v.value().intValue() + 1);
                            result.put(definition.getColumn(keyIndex[i]).getName(), v);
                        }
                    }
                } catch (IOException e) {
                    log.error("Column:{} decode failed", Arrays.stream(context.columns).map(col -> col.name).toArray());
                    return new DingoExecResult(false, "count operation decode failed, " + e.getMessage());
                }
            }
        }
        return new DingoExecResult(result, true, "OK", NumericType.COUNT.name());
    }
}
