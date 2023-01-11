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
import io.dingodb.common.util.Optional;
import io.dingodb.mpu.MPURegister;
import io.dingodb.mpu.storage.Storage;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Core {

    public final CoreMeta meta;

    protected final List<CoreMeta> mirrors;

    protected final Sidebar sidebar;

    @Getter
    @SuppressWarnings("checkstyle:MemberName")
    protected final VCore vCore;

    @SuppressWarnings("checkstyle:MemberName")
    protected Map<CommonId, Core> vCores = new ConcurrentHashMap<>();

    public Core(CoreMeta meta, List<CoreMeta> mirrors, Storage storage, Sidebar sidebar) {
        this.meta = meta;
        this.mirrors = mirrors;
        this.sidebar = sidebar;
        this.vCore = Optional.ofNullable(mirrors)
            .filter(__ -> !__.isEmpty())
            .map(__ -> new VCore(this, meta, __.get(0), __.get(1), storage))
            .ifAbsentSet(() -> new VCore(this, meta, storage))
            .get();
        MPURegister.register(this);
    }

    public Storage getStorage() {
        return vCore.storage;
    }

    public void destroy() {
        vCores.values().forEach(__ -> Optional.or(__.sidebar, __.sidebar::destroy, __::destroy));
        vCore.destroy();
    }

}
