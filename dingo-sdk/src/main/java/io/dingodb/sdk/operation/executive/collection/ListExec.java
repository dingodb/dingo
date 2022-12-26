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
import io.dingodb.sdk.operation.context.Context;
import io.dingodb.sdk.operation.executive.AbstractExecutive;
import io.dingodb.sdk.operation.result.MultiValueOpResult;
import io.dingodb.sdk.operation.unit.collection.ListUnit;
import io.dingodb.server.protocol.CommonIdConstant;

import java.util.Iterator;

@AutoService(Executive.class)
public class ListExec extends AbstractExecutive<Context, Iterator<Object[]>> {

    public static final CommonId COMMON_ID = new CommonId(
        CommonIdConstant.ID_TYPE.op,
        CommonIdConstant.OP_IDENTIFIER.internal,
        CommonIdConstant.ROOT_DOMAIN,
        32);

    @Override
    public CommonId getId() {
        return COMMON_ID;
    }

    @Override
    public MultiValueOpResult execute(Context context, Iterator<Object[]> records) {
        String col = context.column()[0].name;
        ListUnit unit = new ListUnit<>();
        try {
            int keyIndex = context.definition.getColumnIndex(col);
            while (records.hasNext()) {
                Object[] record = records.next();
                unit.add(record[keyIndex]);
            }
        } catch (RuntimeException e) {
            throw new RuntimeException(e);
        }
        return new MultiValueOpResult<>(unit.asList());
    }
}