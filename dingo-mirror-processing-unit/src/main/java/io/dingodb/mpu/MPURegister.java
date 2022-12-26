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

package io.dingodb.mpu;

import io.dingodb.common.CommonId;
import io.dingodb.common.util.Parameters;
import io.dingodb.mpu.api.InternalApi;
import io.dingodb.mpu.api.StorageApi;
import io.dingodb.mpu.core.Core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class MPURegister {

    static {
        InternalApi.load();
        StorageApi.load();
    }

    private MPURegister() {
    }

    private static final Map<CommonId, Core> cores = new ConcurrentHashMap<>();

    public static Core get(CommonId id) {
        Parameters.nonNull(id, "id");
        return cores.get(id);
    }

    public static void register(Core core) {
        Parameters.nonNull(core, "core");
        cores.put(core.meta.coreId, core);
    }

    public static void unregister(CommonId id) {
        Parameters.nonNull(id, "id");
        cores.remove(id);
    }

}
