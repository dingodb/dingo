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

package io.dingodb.client.vector;

import io.dingodb.client.VectorContext;
import io.dingodb.client.common.KeyValueCodec;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.service.IndexService;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.concurrent.atomic.AtomicReference;

@Builder
@Getter
@AllArgsConstructor
public class OperationContext {

    private final long requestId;

    private final DingoCommonId tableId;
    private final DingoCommonId indexId;
    private final DingoCommonId regionId;
    private final KeyValueCodec codec;

    private final IndexService indexService;
    private final VectorContext vectorContext;

    private final int seq;
    private final Any parameters;
    private final AtomicReference<Object> result;

    public <P> P parameters() {
        return parameters.getValue();
    }

    public <R> R result() {
        return (R) result.get();
    }

}
