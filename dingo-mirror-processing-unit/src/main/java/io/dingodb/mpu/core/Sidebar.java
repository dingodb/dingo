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
import io.dingodb.mpu.storage.Storage;
import lombok.Getter;

import java.util.List;

public abstract class Sidebar implements CoreListener {

    @Getter
    protected final Core core;

    public Sidebar(CoreMeta meta, List<CoreMeta> mirrors, Storage storage) {
        this.core = new Core(meta, mirrors, storage, this);
    }

    protected void addVCore(Core core) {
        this.core.vCores.put(core.meta.coreId, core);
    }

    protected Core getVCore(CommonId id) {
        return core.vCores.get(id);
    }

}
