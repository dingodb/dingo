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

package io.dingodb.mpu.core;

import io.dingodb.common.CommonId;
import io.dingodb.common.util.Parameters;
import io.dingodb.mpu.api.InternalApi;
import io.dingodb.mpu.api.StorageApi;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class MPURegister {

    static {
        InternalApi.load();
        StorageApi.load();
    }

    private MPURegister() {
    }

    private static final Map<CommonId, MirrorProcessingUnit> mpus = new ConcurrentHashMap<>();

    public static MirrorProcessingUnit mpu(CommonId id) {
        Parameters.nonNull(id, "id");
        return mpus.get(id);
    }

    public static void put(MirrorProcessingUnit mpu) {
        Parameters.nonNull(mpu, "mpu");
        mpus.put(mpu.id, mpu);
    }

    public static void remove(CommonId id) {
        Parameters.nonNull(id, "id");
        mpus.remove(id);
    }

}
