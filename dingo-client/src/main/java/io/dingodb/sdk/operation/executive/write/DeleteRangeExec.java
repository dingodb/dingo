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
import io.dingodb.common.DingoOpResult;
import io.dingodb.common.Executive;
import io.dingodb.sdk.operation.context.Context;
import io.dingodb.sdk.operation.executive.AbstractExecutive;
import io.dingodb.sdk.operation.result.VoidOpResult;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
@AutoService(Executive.class)
public class DeleteRangeExec extends AbstractExecutive<Context, Void> {

    public static final CommonId COMMON_ID = new CommonId(CommonId.CommonType.OP, 0, 4);

    @Override
    public CommonId getId() {
        return COMMON_ID;
    }

    @Override
    public DingoOpResult<Boolean> execute(Context context, Void record) {
        try {
            context.writer().erase(context.startKey().get(0), context.endKey().get(0));
        } catch (IOException e) {
            log.error("Delete record failed. e", e);
            return new VoidOpResult<>(false);
        }
        return new VoidOpResult<>(true);
    }
}
