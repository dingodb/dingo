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

import io.dingodb.common.operation.Value;
import io.dingodb.common.operation.ExecutiveResult;
import io.dingodb.common.operation.compute.NumericType;
import io.dingodb.common.operation.compute.number.ComputeNumber;
import io.dingodb.common.operation.context.BasicContext;
import io.dingodb.common.store.KeyValue;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Slf4j
public class MaxExecutive extends NumberExecutive<BasicContext, Iterator<KeyValue>, Object> {

    @Override
    public ExecutiveResult execute(BasicContext context, Iterator<KeyValue> records) {
        int[] indexes = new int[context.columns.length];
        for (int i = 0; i < context.columns.length; i++) {
            indexes[i] = context.definition.getColumnIndexOfValue(context.columns[i].name);
        }
        Map<String, Value> result = new HashMap<>();
        Map<String, ComputeNumber> map = new HashMap<>();
        while (records.hasNext()) {
            KeyValue keyValue = records.next();
            boolean flag;
            if (context.filter == null) {
                flag = true;
            } else {
                flag = context.filter.filter(context, keyValue.getValue());
            }
            if (flag) {
                try {
                    Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), indexes);
                    for (int i = 0; i < objects.length; i++) {
                        ComputeNumber number = convertType(objects[i]);
                        map.merge(context.columns[i].name, number, ComputeNumber::max);
                    }
                } catch (IOException e) {
                    log.error("Column:{} decode failed", Arrays.stream(context.columns).map(col -> col.name).toArray());
                    return new ExecutiveResult(null, false, NumericType.MAX.name());
                }
            }
        }
        map.forEach((key, value) -> result.put(key, value.value()));
        return new ExecutiveResult(Collections.singletonList(result), true, NumericType.MAX.name());
    }
}
