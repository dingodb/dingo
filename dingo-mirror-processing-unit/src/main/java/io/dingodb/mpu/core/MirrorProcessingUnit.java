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
import io.dingodb.common.util.PreParameters;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MirrorProcessingUnit {

    public final CommonId id;
    public final String path;

    private final Map<CommonId, Core> subCores = new ConcurrentHashMap<>();

    public MirrorProcessingUnit(CommonId id, String path) {
        this.id = id;
        this.path = path;
        MPURegister.put(this);
    }

    public void core(Core core) {
        subCores.put(core.meta.coreId, core);
    }

    public Core core(CommonId id) {
        PreParameters.nonNull(id, "id");
        return subCores.get(id);
    }


}
