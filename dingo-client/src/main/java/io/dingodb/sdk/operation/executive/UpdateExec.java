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

package io.dingodb.sdk.operation.executive;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.DingoOpResult;
import io.dingodb.common.Executive;
import io.dingodb.common.codec.DingoCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.converter.DingoConverter;
import io.dingodb.sdk.operation.Column;
import io.dingodb.sdk.operation.context.Context;
import io.dingodb.sdk.operation.result.VoidOpResult;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@AutoService(Executive.class)
public class UpdateExec extends AbstractExecutive<Context, Void> {

    public static final CommonId COMMON_ID = new CommonId(CommonId.CommonType.OP, 0, 6);

    @Override
    public CommonId getId() {
        return COMMON_ID;
    }

    @Override
    public DingoOpResult execute(Context context, Void record) {
        Column[] cols = context.column();
        TableDefinition definition = context.definition;
        try {
            List<KeyValue> keyValueList = context.reader().get(context.startKey());

            if (keyValueList.get(0).getValue() == null && !context.isUseDefaultWhenNotExisted()) {
                log.warn("The record is empty and cannot be modified");
                return new VoidOpResult<>(false);
            }
            if (keyValueList.get(0).getValue() == null && context.isUseDefaultWhenNotExisted()) {
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
                        return new VoidOpResult<>(false);
                    }
                }
                for (Column col : cols) {
                    int columnIndexOfValue = definition.getColumnIndexOfValue(col.name);
                    defValues[columnIndexOfValue] = col.value.getObject();
                }

                DingoType schema = definition.getDingoType(false);
                TupleMapping valueMapping = definition.getValueMapping();
                DingoCodec valueCodec = new DingoCodec(schema.toDingoSchemas(), valueMapping);

                Object[] objects = (Object[]) schema.parse(defValues);
                Object[] value = (Object[]) schema.convertTo(objects, DingoConverter.INSTANCE);
                byte[] valueBytes = valueCodec.encode(value);

                context.writer().set(context.startKey().get(0), valueBytes);
            } else {
                DingoCodec valueCodec = new DingoCodec(definition.getDingoSchemaOfValue());
                int[] indexOfValue = new int[cols.length];
                for (int i = 0; i < cols.length; i++) {
                    indexOfValue[i] = definition.getColumnIndexOfValue(cols[i].name);
                }
                Object[] values = new Object[cols.length];
                if (keyValueList.get(0).getValue() != null) {
                    for (KeyValue keyValue : keyValueList) {
                        for (int i = 0; i < cols.length; i++) {
                            DingoType dingoType = Objects.requireNonNull(definition.getColumn(cols[i].name)).getType();
                            values[i] = dingoType.parse(cols[i].value.getObject());
                        }
                        byte[] bytes = valueCodec.encode(keyValue.getValue(), values, indexOfValue);
                        keyValue.setValue(bytes);
                        context.writer().set(keyValue.getKey(), keyValue.getValue());
                    }
                }
            }
        } catch (IOException e) {
            log.error("Update record failed. e", e);
            return new VoidOpResult<>(false);
        }
        return new VoidOpResult<>(true);
    }
}
