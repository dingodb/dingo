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
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.converter.ClientConverter;
import io.dingodb.common.type.converter.DingoConverter;
import io.dingodb.sdk.operation.Column;
import io.dingodb.sdk.operation.context.Context;
import io.dingodb.sdk.operation.executive.AbstractExecutive;
import io.dingodb.sdk.operation.number.ComputeNumber;
import io.dingodb.sdk.operation.result.CollectionOpResult;
import io.dingodb.sdk.operation.result.VoidOpResult;
import io.dingodb.server.protocol.CommonIdConstant;
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

    public static final CommonId COMMON_ID = new CommonId(
        CommonIdConstant.ID_TYPE.op,
        CommonIdConstant.OP_IDENTIFIER.internal,
        CommonIdConstant.ROOT_DOMAIN,
        30);

    @Override
    public CommonId getId() {
        return COMMON_ID;
    }

    @Override
    public CollectionOpResult execute(Context context, Iterator<Object[]> records) {
        Column col = context.column();
        List<Object[]> result = new ArrayList<>();
        try {
            int keyIndex = context.definition.getColumnIndex(col.name);
            KeyValueCodec codec = context.keyValueCodec();
            if (!records.hasNext() && context.isUseDefaultWhenNotExisted()) {
                List<ColumnDefinition> columnDefinitions = context.definition.getColumns()
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
                int indexOfValue = context.definition.getColumnIndexOfValue(col.name);
                DingoType dingoType = Objects.requireNonNull(context.definition.getColumn(col.name)).getType();
                Object v = col.value.getObject();
                ComputeNumber oldNum = convertType(v == null ? 0 : v, dingoType);
                Object defValue = defValues[indexOfValue];
                ComputeNumber newValue = oldNum.add(convertType(defValue == null ? 0 : defValue, dingoType));
                defValues[indexOfValue] = newValue.value();

                DingoType schema = context.definition.getDingoType(false);
                TupleMapping valueMapping = context.definition.getValueMapping();
                DingoCodec valueCodec = new DingoCodec(schema.toDingoSchemas(), valueMapping);

                Object[] objects = (Object[]) schema.parse(defValues);
                Object[] value = (Object[]) schema.convertTo(objects, DingoConverter.INSTANCE);
                byte[] valueBytes = valueCodec.encode(value);

                context.writer().set(context.startKey().get(0), valueBytes);
                result.add(defValues);
            } else {
                while (records.hasNext()) {
                    Object[] record = records.next();
                    DingoType dingoType = context.definition.getColumn(keyIndex).getType();
                    Object v = col.value.getObject();
                    ComputeNumber oldNum1 = convertType(v == null ? 0 : v, dingoType);
                    ComputeNumber oldNum2 = convertType(record[keyIndex], dingoType);
                    ComputeNumber newNum = oldNum1.add(oldNum2);
                    record[keyIndex] = dingoType.convertFrom(newNum.value(), ClientConverter.INSTANCE);
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
