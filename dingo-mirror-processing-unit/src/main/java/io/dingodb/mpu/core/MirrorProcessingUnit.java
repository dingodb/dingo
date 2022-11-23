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
import io.dingodb.mpu.storage.rocks.RocksStorage;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MirrorProcessingUnit {

    public final CommonId id;
    public final Path path;
    public final String dbRocksOptionsFile;
    public final String logRocksOptionsFile;
    public final int ttl;

    private final Map<CommonId, Core> subCores = new ConcurrentHashMap<>();

    public MirrorProcessingUnit(CommonId id, Path path, String dbRocksOptionsFile, String logRocksOptionsFile) {
        this(id, path, dbRocksOptionsFile, logRocksOptionsFile, 0);
    }

    public MirrorProcessingUnit(CommonId id, Path path, final String dbRocksOptionsFile,
                                final String logRocksOptionsFile, final int ttl) {
        this.id = id;
        this.path = path;
        this.dbRocksOptionsFile = dbRocksOptionsFile;
        this.logRocksOptionsFile = logRocksOptionsFile;
        this.ttl = ttl;
        MPURegister.put(this);
    }

    public void core(Core core) {
        subCores.put(core.meta.coreId, core);
    }

    public Core core(CommonId id) {
        Parameters.nonNull(id, "id");
        return subCores.get(id);
    }

    public Core createCore(int num, List<CoreMeta> metas) {
        try {
            Core core;
            CoreMeta local = metas.remove(num);
            log.info("Create core {} for {}", local.coreId, id);
            RocksStorage storage = new RocksStorage(local, path.resolve(local.coreId.toString()).toString(),
                this.dbRocksOptionsFile, this.logRocksOptionsFile, this.ttl);
            if (metas.size() == 0) {
                core = new Core(this, local, null, null, storage);
            } else if (metas.size() == 2) {
                core = new Core(this, local, metas.get(0), metas.get(1), storage);
            } else {
                throw new IllegalArgumentException("Mirror size must 0 or 2, but " + metas.size());
            }
            subCores.put(local.coreId, core);
            log.info("Create core {} for {} success", local.coreId, id);
            return core;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
