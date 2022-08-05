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
import io.dingodb.common.operation.compute.number.ComputeNumber;
import io.dingodb.common.operation.context.BasicContext;
import io.dingodb.common.store.KeyValue;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Slf4j
public class AddExecutive extends NumberExecutive<BasicContext, Iterator<KeyValue>, Object> {

    @Override
    public List<KeyValue> execute(BasicContext context, Iterator<KeyValue> records) {
        List<KeyValue> list = new ArrayList<>();
        while (records.hasNext()) {
            KeyValue keyValue = records.next();
            boolean flag;
            if (context.filter == null) {
                flag = true;
            } else {
                flag = context.filter.filter(context, keyValue.getValue());
            }
            if (flag) {
                for (Column column : context.columns) {
                    int index = context.definition.getColumnIndexOfValue(column.name);
                    try {
                        Object value = context.dingoValueCodec().decode(keyValue.getValue(), new int[]{index})[0];
                        ComputeNumber record = convertType(column.value.getObject());
                        ComputeNumber add = record.add(convertType(value));

                        byte[] bytes = context.dingoValueCodec()
                            .encode(keyValue.getValue(), new Object[]{add.value().getObject()}, new int[]{index});
                        keyValue.setValue(bytes);
                        list.add(keyValue);
                    } catch (IOException e) {
                        log.error("Column:{} decode/encode failed", column.name, e);
                    }
                }
            }
        }
        return list;
    }

}
