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

import io.dingodb.common.codec.DingoCodec;
import io.dingodb.common.operation.Column;
import io.dingodb.common.operation.compute.number.ComputeNumber;
import io.dingodb.common.operation.context.BasicContext;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.converter.ClientConverter;
import io.dingodb.common.type.converter.DingoConverter;
import io.dingodb.common.util.ByteArrayUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class AddExecutive extends NumberExecutive<BasicContext, Iterator<KeyValue>, Object> {

    @Override
    public List<KeyValue> execute(BasicContext context, Iterator<KeyValue> records) {
        List<KeyValue> list = new ArrayList<>();
        try {
            Column[] columns = context.columns;
            TableDefinition definition = context.definition;
            int[] indexes = new int[columns.length];
            for (int i = 0; i < columns.length; i++) {
                indexes[i] = definition.getColumnIndexOfValue(columns[i].name);
            }
            int totalCnt = 0;
            while (records.hasNext()) {
                KeyValue keyValue = records.next();
                totalCnt++;
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
                            DingoType dingoType =
                                Objects.requireNonNull(definition.getColumn(columns[i].name)).getDingoType();
                            Object v = columns[i].value.getObject();
                            ComputeNumber number = convertType(v == null ? 0 : v, dingoType);
                            ComputeNumber value = number.add(convertType(objects[i], dingoType));
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
            int compare = ByteArrayUtils.compare(context.primaryStartKey, context.primaryEndKey);
            if (context.useDefaultWhenNotExisted && totalCnt == 0 && (compare == -1 || compare == 0)) {
                List<ColumnDefinition> columnDefinitions = definition.getColumns()
                    .stream()
                    .filter(c -> !c.isPrimary())
                    .collect(Collectors.toList());
                Object[] defValues = columnDefinitions.stream()
                    .map(ColumnDefinition::getDefaultValue)
                    .toArray();
                for (ColumnDefinition column : columnDefinitions) {
                    if (column.isNotNull() && column.getDefaultValue() == null) {
                        throw new IllegalArgumentException("Missing default value for fields that do not allow null");
                    }
                }
                for (int i = 0; i < indexes.length; i++) {
                    DingoType dingoType = Objects.requireNonNull(definition.getColumn(columns[i].name)).getDingoType();
                    Object v = columns[i].value.getObject();
                    ComputeNumber number = convertType(v == null ? 0 : v, dingoType);
                    Object defValue = defValues[indexes[i]];
                    ComputeNumber value = number.add(convertType(defValue == null ? 0 : defValue, dingoType));
                    defValues[indexes[i]] = value.value().getObject();
                }
                DingoType schema = definition.getDingoType(false);
                TupleMapping valueMapping = definition.getValueMapping();
                DingoCodec valueCodec = new DingoCodec(schema.toDingoSchemas(), valueMapping);

                Object[] objects = (Object[]) schema.parse(defValues);
                Object[] value = (Object[]) schema.convertTo(objects, DingoConverter.INSTANCE);
                byte[] bytes = valueCodec.encode(value);

                KeyValue keyValue = new KeyValue(context.primaryStartKey, bytes);
                list.add(keyValue);
            }
        } catch (Exception e) {
            log.error("add failed to execute, e: ", e);
        }
        return list;
    }

}
