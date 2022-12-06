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
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.type.converter.ClientConverter;
import io.dingodb.common.type.converter.DingoConverter;
import io.dingodb.sdk.operation.Column;
import io.dingodb.sdk.operation.context.Context;
import io.dingodb.sdk.operation.result.VoidOpResult;
import io.dingodb.server.protocol.CommonIdConstant;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@AutoService(Executive.class)
public class UpdateExec extends AbstractExecutive<Context, Void> {

    public static final CommonId COMMON_ID = new CommonId(
        CommonIdConstant.ID_TYPE.op,
        CommonIdConstant.OP_IDENTIFIER.internal,
        CommonIdConstant.ROOT_DOMAIN,
        6);

    @Override
    public CommonId getId() {
        return COMMON_ID;
    }

    @Override
    public DingoOpResult execute(Context context, Void record) {
        Column col = context.column();
        int columnIndex = context.definition.getColumnIndex(col.name);
        int indexOfValue = context.definition.getColumnIndexOfValue(col.name);
        try {
            byte[] bytes = context.reader().get(context.startKey().get(0));
            if (bytes == null && !context.isUseDefaultWhenNotExisted()) {
                log.warn("The record is empty and cannot be modified");
                return new VoidOpResult<>(false);
            }
            if (bytes == null && context.isUseDefaultWhenNotExisted()) {
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
                        return new VoidOpResult<>(false);
                    }
                }
                defValues[indexOfValue] = col.value.getObject();

                DingoType schema = context.definition.getDingoType(false);
                TupleMapping valueMapping = context.definition.getValueMapping();
                DingoCodec valueCodec = new DingoCodec(schema.toDingoSchemas(), valueMapping);

                Object[] objects = (Object[]) schema.parse(defValues);
                Object[] value = (Object[]) schema.convertTo(objects, DingoConverter.INSTANCE);
                byte[] valueBytes = valueCodec.encode(value);

                context.writer().set(context.startKey().get(0), valueBytes);
            } else {
                DingoCodec valueCodec = new DingoCodec(context.definition.getDingoSchemaOfValue());
                Object[] oldValue = valueCodec.decode(bytes, new int[]{indexOfValue});
                oldValue[0] = col.value.getObject();
                DingoType dingoType = DingoTypeFactory.tuple(TupleMapping.of(new int[]{columnIndex}).stream()
                    .mapToObj(context.definition.getColumns()::get)
                    .map(ColumnDefinition::getType)
                    .toArray(DingoType[]::new));
                Object[] converted = (Object[]) dingoType.convertFrom(oldValue, ClientConverter.INSTANCE);
                bytes = valueCodec.encode(bytes, converted, new int[]{indexOfValue});

                context.writer().set(context.startKey().get(0), bytes);
            }
        } catch (IOException e) {
            log.error("Update record failed. e", e);
            return new VoidOpResult<>(false);
        }
        return new VoidOpResult<>(true);
    }
}
