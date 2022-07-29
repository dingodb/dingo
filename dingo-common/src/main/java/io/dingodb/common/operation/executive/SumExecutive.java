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

import io.dingodb.common.operation.Column;
import io.dingodb.common.operation.ExecutiveResult;
import io.dingodb.common.operation.Value;
import io.dingodb.common.operation.compute.number.ComputeNumber;
import io.dingodb.common.operation.context.BasicContext;
import io.dingodb.common.store.KeyValue;
import org.luaj.vm2.LuaValue;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SumExecutive extends NumberExecutive<BasicContext, Iterator<KeyValue>, Object> {

    @Override
    public ExecutiveResult execute(BasicContext context, Iterator<KeyValue> records) {
        /*int[] indexes = new int[context.columns.length];
        for (int i = 0; i < context.columns.length; i++) {
            Column column = context.columns[i];
            indexes[i] = context.definition.getColumnIndexOfValue(column.name);
        }*/
        Map<String, ComputeNumber> map = new HashMap<>();

        Map<String, Value> result = new HashMap<>();
        Column column = context.columns[0];
        int index = context.definition.getColumnIndexOfValue(column.name);


        while (records.hasNext()) {
            KeyValue keyValue = records.next();
            try {

                // Object[] objects = context.keyValueCodec().decode(keyValue);
                //LuaTable recordTable = LuaJUtils.getLuaTable(context.definition.getDingoSchema(), objects);
                LuaValue udf = context.globals.get(LuaValue.valueOf(context.filter.getFunctionName()));
                Object value = context.dingoCodec().decode(keyValue.getValue(), new int[]{index})[0];
                LuaValue functionResult = udf.call(convertToLuaValue(value));
                boolean filter = functionResult.toboolean();
                if (filter) {
                    ComputeNumber record = convertType(value);
                    ComputeNumber temp = map.get(column.name);
                    if (temp == null) {
                        map.put(column.name, record);
                    } else {
                        temp.add(record);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        map.forEach((key, value) -> result.put(key, value.value()));
        return new ExecutiveResult(Collections.singletonList(result), true);
    }
}
