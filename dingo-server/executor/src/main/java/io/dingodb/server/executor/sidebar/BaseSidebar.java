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

package io.dingodb.server.executor.sidebar;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.mpu.storage.Storage;
import io.dingodb.server.api.ServiceConnectApi;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class BaseSidebar extends io.dingodb.mpu.core.Sidebar implements ServiceConnectApi.Service {
    public BaseSidebar(CoreMeta meta, List<CoreMeta> mirrors, Storage storage) {
        super(meta, mirrors, storage);
        ServiceConnectApi.INSTANCE.register(this);
    }

    public void destroy() {
        ServiceConnectApi.INSTANCE.unregister(id());
    }

    @Override
    public CommonId id() {
        return core.meta.coreId;
    }

    @Override
    public Location leader() {
        return core.getPrimary().location;
    }

    @Override
    public List<Location> getAll() {
        List<Location> locations = new ArrayList<>();
        locations.add(DingoConfiguration.location());
        locations.addAll(core.mirrors().stream().map(CoreMeta::location).collect(Collectors.toList()));
        return locations;
    }

}
