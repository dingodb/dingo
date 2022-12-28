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
import io.dingodb.sdk.operation.Row;
import io.dingodb.sdk.operation.context.Context;
import io.dingodb.sdk.operation.executive.AbstractExecutive;
import io.dingodb.sdk.operation.result.MultiValueOpResult;
import io.dingodb.sdk.operation.unit.collection.SortedListUnit;
import io.dingodb.server.protocol.CommonIdConstant;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;

@Slf4j
@AutoService(Executive.class)
public class SortListExec extends AbstractExecutive<Context, Iterator<Object[]>> {

    public static final CommonId COMMON_ID = new CommonId(
        CommonIdConstant.ID_TYPE.op,
        CommonIdConstant.OP_IDENTIFIER.internal,
        CommonIdConstant.ROOT_DOMAIN,
        33);

    @Override
    public CommonId getId() {
        return COMMON_ID;
    }

    @Override
    public MultiValueOpResult execute(Context context, Iterator<Object[]> records) {
        String col = context.column()[0].name;
        SortedListUnit unit = new SortedListUnit<>();
        try {
            int keyIndex = context.definition.getColumnIndex(col);
            while (records.hasNext()) {
                Object[] record = records.next();
                Row row = new Row(record, (Comparable) record[keyIndex]);
                unit.add(row);
            }

        } catch (RuntimeException e) {
            throw new RuntimeException(e);
        }
        return new MultiValueOpResult<>(unit);
    }
}
