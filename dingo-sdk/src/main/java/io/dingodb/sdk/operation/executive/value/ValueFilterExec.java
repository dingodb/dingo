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
import io.dingodb.sdk.operation.context.Context;
import io.dingodb.sdk.operation.executive.AbstractExecutive;
import io.dingodb.sdk.operation.filter.DingoFilter;
import io.dingodb.sdk.operation.result.ValueOpResult;
import io.dingodb.sdk.operation.unit.numeric.NumberUnit;
import io.dingodb.server.protocol.CommonIdConstant;

@AutoService(Executive.class)
public class ValueFilterExec extends AbstractExecutive<Context, NumberUnit> {

    public static final CommonId COMMON_ID = new CommonId(
        CommonIdConstant.ID_TYPE.op,
        CommonIdConstant.OP_IDENTIFIER.internal,
        0,
        22);

    @Override
    public CommonId getId() {
        return COMMON_ID;
    }

    @Override
    public DingoOpResult execute(Context context, NumberUnit record) {
        DingoFilter filter = context.filter();
        if (filter.filter(record.getValue().value())) {
            return new ValueOpResult<>(record);
        }
        return new ValueOpResult<>(null);
    }
}
