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

package io.dingodb.sdk.operation.executive.collection;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.Executive;
import io.dingodb.common.codec.DingoCodec;
import io.dingodb.common.codec.KeyValueCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.converter.ClientConverter;
import io.dingodb.common.type.converter.DingoConverter;
import io.dingodb.sdk.operation.Column;
import io.dingodb.sdk.operation.context.Context;
import io.dingodb.sdk.operation.executive.AbstractExecutive;
import io.dingodb.sdk.operation.number.ComputeNumber;
import io.dingodb.sdk.operation.result.CollectionOpResult;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@AutoService(Executive.class)
public class CollAddExec extends AbstractExecutive<Context, Iterator<Object[]>> {

    public static final CommonId COMMON_ID = new CommonId(CommonId.CommonType.OP, 0, 30);

    @Override
    public CommonId getId() {
        return COMMON_ID;
    }

    @Override
    public CollectionOpResult execute(Context context, Iterator<Object[]> records) {
        Column[] cols = context.column();
        TableDefinition definition = context.definition;
        List<Object[]> result = new ArrayList<>();
        try {
            int[] indexes = new int[cols.length];
            int[] indexOfValue = new int[cols.length];
            for (int i = 0; i < cols.length; i++) {
                indexes[i] = definition.getColumnIndex(cols[i].name);
                indexOfValue[i] = definition.getColumnIndexOfValue(cols[i].name);
            }
            KeyValueCodec codec = context.keyValueCodec();
            if (!records.hasNext() && context.isUseDefaultWhenNotExisted()) {
                List<ColumnDefinition> columnDefinitions = definition.getColumns()
                    .stream()
                    .filter(c -> !c.isPrimary())
                    .collect(Collectors.toList());
                Object[] defValues = columnDefinitions
                    .stream()
                    .map(ColumnDefinition::getDefaultValue)
                    .toArray();
                for (ColumnDefinition column : columnDefinitions) {
                    if (!column.isNullable() && column.getDefaultValue() == null) {
                        log.error("Missing default value for fields that do not allow null");
                        return new CollectionOpResult<>(Collections.emptyIterator());
                    }
                }
                for (int i = 0; i < indexOfValue.length; i++) {
                    DingoType dingoType = Objects.requireNonNull(definition.getColumn(cols[i].name)).getType();
                    Object v = cols[i].value.getObject();
                    ComputeNumber number = convertType(v == null ? 0 : v, dingoType);
                    Object defValue = defValues[indexOfValue[i]];
                    ComputeNumber newNum = number.add(convertType(defValue == null ? 0 : defValue, dingoType));
                    defValues[indexOfValue[i]] = newNum.value();
                }

                DingoType schema = definition.getDingoType(false);
                TupleMapping valueMapping = definition.getValueMapping();
                DingoCodec valueCodec = new DingoCodec(schema.toDingoSchemas(), valueMapping);

                Object[] objects = (Object[]) schema.parse(defValues);
                Object[] value = (Object[]) schema.convertTo(objects, DingoConverter.INSTANCE);
                byte[] valueBytes = valueCodec.encode(value);

                context.writer().set(context.startKey().get(0), valueBytes);

                Object[] keys = context.keyValueCodec().decodeKey(context.startKey().get(0));
                Object[] dest = new Object[keys.length + value.length];
                System.arraycopy(keys, 0, dest, 0, keys.length);
                System.arraycopy(value, 0, dest, keys.length, value.length);
                result.add(dest);
            } else {
                while (records.hasNext()) {
                    Object[] record = records.next();
                    for (int i = 0; i < indexes.length; i++) {
                        if (record[indexes[i]] == null) {
                            continue;
                        }
                        DingoType dingoType = definition.getColumn(indexes[i]).getType();
                        Object v = cols[i].value.getObject();
                        ComputeNumber number = convertType(v == null ? 0 : v, dingoType);
                        ComputeNumber newNum = number.add(convertType(record[indexes[i]], dingoType));
                        record[indexes[i]] = dingoType.convertFrom(newNum.value(), ClientConverter.INSTANCE);
                    }
                    KeyValue keyValue = codec.encode(record);
                    context.writer().set(keyValue.getKey(), keyValue.getValue());
                    result.add(record);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new CollectionOpResult<>(result.iterator());
    }

}
