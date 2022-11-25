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
import io.dingodb.common.codec.KeyValueCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.converter.ClientConverter;
import io.dingodb.sdk.operation.Column;
import io.dingodb.sdk.operation.context.Context;
import io.dingodb.sdk.operation.executive.AbstractExecutive;
import io.dingodb.sdk.operation.number.ComputeNumber;
import io.dingodb.sdk.operation.result.CollectionOpResult;
import io.dingodb.server.protocol.CommonIdConstant;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Slf4j
@AutoService(Executive.class)
public class CollAddExec extends AbstractExecutive<Context, Iterator<Object[]>> {

    public static final CommonId COMMON_ID = new CommonId(
        CommonIdConstant.ID_TYPE.op,
        CommonIdConstant.OP_IDENTIFIER.internal,
        0,
        30);

    @Override
    public CommonId getId() {
        return COMMON_ID;
    }

    @Override
    public CollectionOpResult execute(Context context, Iterator<Object[]> records) {
        Column column = context.column();
        List<Object[]> result = new ArrayList<>();
        try {
            int keyIndex = context.definition.getColumnIndex(column.name);
            KeyValueCodec codec = context.keyValueCodec();
            while (records.hasNext()) {
                Object[] record = records.next();
                DingoType dingoType = context.definition.getColumn(keyIndex).getType();
                Object v = column.value.getObject();
                ComputeNumber oldNum1 = convertType(v == null ? 0 : v, dingoType);
                ComputeNumber oldNum2 = convertType(record[keyIndex], dingoType);
                ComputeNumber newNum = oldNum1.add(oldNum2);
                record[keyIndex] = dingoType.convertFrom(newNum.value(), ClientConverter.INSTANCE);
                KeyValue keyValue = codec.encode(record);
                context.writer().set(keyValue.getKey(), keyValue.getValue());
                result.add(record);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new CollectionOpResult<>(result.iterator());
    }

}
