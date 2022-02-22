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

package io.dingodb.store.row;

import io.dingodb.common.table.TableId;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.row.client.DefaultDingoRowStore;
import io.dingodb.store.row.errors.DingoRowStoreRuntimeException;
import io.dingodb.store.row.options.DingoRowStoreOptions;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;

@Slf4j
public class RowStoreInstance implements StoreInstance {
    private final Map<byte[], RowPartitionOper> blockMap;
    private static volatile DefaultDingoRowStore kvStore;
    private static DingoRowStoreOptions rowStoreOptions;

    public RowStoreInstance(String path) {
        this.blockMap = new LinkedHashMap<>();
    }

    public static void setRowStoreOptions(DingoRowStoreOptions opts) {
        rowStoreOptions = opts;
    }

    public static DefaultDingoRowStore kvStore() {
        if (kvStore == null) {
            synchronized (DefaultDingoRowStore.class) {
                if (kvStore == null) {
                    kvStore = new DefaultDingoRowStore();
                    if (!kvStore.init(rowStoreOptions)) {
                        throw new DingoRowStoreRuntimeException("Fail to start [DefaultDingoRowStore].");
                    }
                }
            }
        }
        return kvStore;
    }

    @Override
    public synchronized RowPartitionOper getKvBlock(@Nonnull TableId tableId, Object partId, boolean isMain) {
        // `partId` is not used for `DefaultDingoRowStore`.
        return blockMap.computeIfAbsent(
            tableId.getValue(),
            value -> new RowPartitionOper(kvStore, tableId.getValue())
        );
    }

}
