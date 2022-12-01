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

package io.dingodb.sdk.operation.executive.write;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.Executive;
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.sdk.operation.Writer;
import io.dingodb.sdk.operation.context.Context;
import io.dingodb.sdk.operation.executive.AbstractExecutive;
import io.dingodb.sdk.operation.result.VoidOpResult;
import io.dingodb.server.protocol.CommonIdConstant;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@AutoService(Executive.class)
public class PutExec extends AbstractExecutive<Context, Void> {

    public static final CommonId COMMON_ID = new CommonId(
        CommonIdConstant.ID_TYPE.op,
        CommonIdConstant.OP_IDENTIFIER.internal,
        0,
        1);

    @Override
    public CommonId getId() {
        return COMMON_ID;
    }

    @Override
    public VoidOpResult<Boolean> execute(Context context, Void record) {

        try {
            List<KeyValue> keyValues = context.recordList();
            if (context.isSkippedWhenExisted()) {
                keyValues = context.recordList().stream()
                    .filter(r -> context.reader().get(r.getKey()) != null)
                    .collect(Collectors.toList());
            }
            if (!keyValues.isEmpty()) {
                Writer writer = context.writer();
                Iterator<KeyValue> iterator = keyValues.iterator();
                int timestamp = context.timestamp();
                if (timestamp > 0) {
                    while (iterator.hasNext()) {
                        KeyValue keyValue = iterator.next();
                        byte[] value = keyValue.getValue();
                        byte[] valueWithTs = PrimitiveCodec.encodeInt(
                            timestamp, ByteArrayUtils.unsliced(value, 0, value.length + 4), value.length, false);
                        writer.set(keyValue.getKey(), valueWithTs);
                    }
                } else {
                    while (iterator.hasNext()) {
                        KeyValue keyValue = iterator.next();
                        writer.set(keyValue.getKey(), keyValue.getValue());
                    }
                }

            }
        } catch (RuntimeException e) {
            log.error("Put record failed. e", e);
            return new VoidOpResult<>(false);
        }
        return new VoidOpResult<>(true);
    }
}
