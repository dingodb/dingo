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

package io.dingodb.sdk.operation.executive.value;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.DingoOpResult;
import io.dingodb.common.Executive;
import io.dingodb.common.type.DingoType;
import io.dingodb.sdk.operation.context.Context;
import io.dingodb.sdk.operation.executive.AbstractExecutive;
import io.dingodb.sdk.operation.result.ValueOpResult;
import io.dingodb.sdk.operation.unit.numeric.IncreaseCountUnit;
import io.dingodb.sdk.operation.unit.numeric.NumberUnit;
import io.dingodb.server.protocol.CommonIdConstant;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.activation.UnsupportedDataTypeException;

@Slf4j
@AutoService(Executive.class)
public class IncreaseCountExec extends AbstractExecutive<Context, Iterator<Object[]>> {

    public static final CommonId COMMON_ID = new CommonId(
        CommonIdConstant.ID_TYPE.op,
        CommonIdConstant.OP_IDENTIFIER.internal,
        CommonIdConstant.ROOT_DOMAIN,
        17);

    @Override
    public CommonId getId() {
        return COMMON_ID;
    }

    @Override
    public DingoOpResult execute(Context context, Iterator<Object[]> records) {
        String col = context.column()[0].name;
        Map<String, NumberUnit> map = new HashMap<>();
        try {
            int keyIndex = context.definition.getColumnIndex(col);
            while (records.hasNext()) {
                Object[] record = records.next();
                DingoType dingoType = context.definition.getColumn(keyIndex).getType();
                NumberUnit unit = new IncreaseCountUnit<>(convertType(record[keyIndex], dingoType));
                map.merge(col, unit, NumberUnit::merge);
            }
        } catch (UnsupportedDataTypeException e) {
            log.error("Unsupported type, e", e);
        }
        return new ValueOpResult(map.get(col));
    }
}
