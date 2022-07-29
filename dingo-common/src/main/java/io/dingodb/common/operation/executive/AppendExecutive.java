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
import io.dingodb.common.operation.compute.str.ComputeString;
import io.dingodb.common.operation.context.BasicContext;
import io.dingodb.common.store.KeyValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AppendExecutive extends KVExecutive<BasicContext, Iterator<KeyValue>, Object> {

    @Override
    public List<KeyValue> execute(BasicContext context, Iterator<KeyValue> records) {
        List<KeyValue> list = new ArrayList<>();
        Column column = context.columns[0];
        int index = context.definition.getColumnIndexOfValue(column.name);
        while (records.hasNext()) {
            KeyValue keyValue = records.next();
            try {
                // todo  check type by schema
                Object value = context.dingoCodec().decode(keyValue.getValue(), new int[]{index})[0];
                ComputeString append = new ComputeString(value.toString());
                append.append(new ComputeString(column.value.toString()));

                byte[] bytes = context.dingoCodec().encode(keyValue.getValue(), new Object[]{append.value().getObject()}, new int[]{index});
                KeyValue kv = new KeyValue(keyValue.getKey(), bytes);
                list.add(kv);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return list;
    }
}
