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
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.converter.ClientConverter;
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
        try {
            Column[] columns = context.columns;
            int[] indexes = new int[columns.length];
            for (int i = 0; i < columns.length; i++) {
                indexes[i] = context.definition.getColumnIndexOfValue(columns[i].name);
            }
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
                        Object[] objects = context.dingoValueCodec().decode(keyValue.getValue(), indexes);
                        Object[] values = new Object[objects.length];
                        for (int i = 0; i < objects.length; i++) {
                            ComputeNumber number = convertType(columns[i].value.getObject());
                            ComputeNumber value = number.add(convertType(objects[i]));
                            values[i] = value.value().getObject();
                        }
                        DingoType dingoType = getDingoType(context);
                        Object[] converted = (Object[]) dingoType.convertFrom(values, ClientConverter.INSTANCE);
                        byte[] bytes = context.dingoValueCodec().encode(keyValue.getValue(), converted, indexes);
                        keyValue.setValue(bytes);
                        list.add(keyValue);
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        } catch (Exception e) {
            log.error("add failed to execute, e: ", e);
        }
        return list;
    }

}
