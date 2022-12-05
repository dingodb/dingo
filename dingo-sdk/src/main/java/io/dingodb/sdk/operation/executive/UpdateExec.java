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
import io.dingodb.common.store.KeyValue;
import io.dingodb.sdk.operation.Column;
import io.dingodb.sdk.operation.context.Context;
import io.dingodb.sdk.operation.result.VoidOpResult;
import io.dingodb.server.protocol.CommonIdConstant;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

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
        try {
            byte[] bytes = context.reader().get(context.startKey().get(0));
            Object[] oldValue = context.keyValueCodec()
                .mapKeyAndDecodeValue(context.startPrimaryKeys.get(0).getUserKey().toArray(), bytes);
            oldValue[columnIndex] = col.value.getObject();
            KeyValue keyValue = context.keyValueCodec().encode(oldValue);

            context.writer().set(keyValue.getKey(), keyValue.getValue());
        } catch (IOException e) {
            log.error("Update record failed. e", e);
            return new VoidOpResult<>(false);
        }
        return new VoidOpResult<>(true);
    }
}
