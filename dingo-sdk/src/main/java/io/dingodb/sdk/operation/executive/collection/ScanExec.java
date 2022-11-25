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
import io.dingodb.common.DingoOpResult;
import io.dingodb.common.Executive;
import io.dingodb.common.codec.KeyValueCodec;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.sdk.operation.context.Context;
import io.dingodb.sdk.operation.executive.AbstractExecutive;
import io.dingodb.sdk.operation.result.CollectionOpResult;
import io.dingodb.server.protocol.CommonIdConstant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@AutoService(Executive.class)
public class ScanExec extends AbstractExecutive<Context, Void> {

    public static final CommonId COMMON_ID = new CommonId(
        CommonIdConstant.ID_TYPE.op,
        CommonIdConstant.OP_IDENTIFIER.internal,
        0,
        0
    );

    @Override
    public CommonId getId() {
        return COMMON_ID;
    }

    @Override
    public DingoOpResult execute(Context context, Void vo) {
        KeyValueCodec codec = context.keyValueCodec();
        List<Object[]> list = new ArrayList<>();

        try {
            byte[] start = context.startKey().get(0);
            byte[] end = context.endKey().get(0);
            Iterator<KeyValue> iterator = context.reader()
                .scan(start, end, true, ByteArrayUtils.equal(start, end));
            iterator.forEachRemaining(kv -> {
                try {
                    list.add(codec.decode(kv));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new CollectionOpResult<>(list.iterator());
    }
}
