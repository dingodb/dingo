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

import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.mpu.core.Sidebar;
import io.dingodb.mpu.storage.rocks.RocksStorage;
import io.dingodb.server.executor.store.StorageFactory;
import io.dingodb.server.protocol.meta.TablePart;

import java.nio.file.Path;
import java.util.List;

public class PartitionSidebar extends BaseSidebar {

    public final TablePart part;

    public PartitionSidebar(TablePart part, CoreMeta meta, List<CoreMeta> mirrors, Path path) throws Exception {
        super(meta, mirrors, StorageFactory.create(meta.label, path));
        this.part = part;
    }

}
