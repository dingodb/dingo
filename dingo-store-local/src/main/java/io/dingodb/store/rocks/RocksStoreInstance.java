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

package io.dingodb.store.rocks;

import io.dingodb.common.table.TupleMapping;
import io.dingodb.common.table.TupleSchema;
import io.dingodb.store.StoreInstance;
import io.dingodb.store.TablePart;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;

@Slf4j
public class RocksStoreInstance implements StoreInstance {
    private final String path;

    private final Map<String, TablePart> partMap;

    public RocksStoreInstance(String path) {
        this.path = path;
        this.partMap = new LinkedHashMap<>();
    }

    @Nonnull
    public String partDir(@Nonnull String tableName, @Nonnull Object partId) {
        return path + File.separator
            + tableName.replace(".", File.separator).toLowerCase() + File.separator
            + partId;
    }

    @Override
    public TablePart createTablePart(
        String tableName,
        Object partId,
        TupleSchema schema,
        TupleMapping keyMapping,
        boolean isLeader
    ) {
        String partDir = partDir(tableName, partId);
        TablePart part = new RocksPart(partDir, schema, keyMapping);
        partMap.put(partDir, part);
        return part;
    }

    @Override
    public TablePart getTablePart(
        String tableName,
        Object partId,
        TupleSchema schema,
        TupleMapping keyMapping
    ) {
        String partDir = partDir(tableName, partId);
        TablePart part = partMap.get(partDir);
        if (part == null) {
            part = new RocksPart(partDir, schema, keyMapping);
            partMap.put(partDir, part);
        }
        return part;
    }
}
