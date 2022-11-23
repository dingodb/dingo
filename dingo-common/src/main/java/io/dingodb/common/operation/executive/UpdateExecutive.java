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
import io.dingodb.common.operation.context.BasicContext;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

@Slf4j
public class UpdateExecutive extends BasicExecutive<BasicContext, Iterator<KeyValue>, Object> {

    @Override
    public List<KeyValue> execute(BasicContext context, Iterator<KeyValue> record) {
        List<KeyValue> list = new ArrayList<>();
        try {
            Column[] columns = context.columns;
            TableDefinition definition = context.definition;
            int[] indexes = new int[columns.length];
            Object[] values = new Object[columns.length];
            for (int i = 0; i < columns.length; i++) {
                indexes[i] = definition.getColumnIndexOfValue(columns[i].name);
            }
            while (record.hasNext()) {
                KeyValue keyValue = record.next();
                boolean filter;
                if (context.filter == null) {
                    filter = true;
                } else {
                    filter = context.filter.filter(context, keyValue);
                }
                if (filter) {
                    try {
                        for (int i = 0; i < columns.length; i++) {
                            DingoType dingoType =
                                Objects.requireNonNull(definition.getColumn(columns[i].name)).getType();
                            values[i] = dingoType.parse(columns[i].value.getObject());
                        }
                        byte[] bytes = context.dingoValueCodec().encode(keyValue.getValue(), values, indexes);
                        keyValue.setValue(bytes);
                        list.add(keyValue);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        } catch (Exception ex) {
            log.error("Field update execution failed, e: ", ex);
        }
        return list;
    }
}
