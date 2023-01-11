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

package io.dingodb.server.coordinator.api;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.protocol.meta.Table;
import io.dingodb.server.protocol.meta.TablePart;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TableApi implements io.dingodb.server.api.TableApi {

    public static final TableApi INSTANCE = new TableApi();

    private TableApi() {
    }

    @Override
    public TableDefinition getDefinition(CommonId tableId) {
        return MetaAdaptorRegistry.getMetaAdaptor(tableId).getDefinition();
    }

    @Override
    public List<TablePart> partitions(CommonId tableId) {
        return Collections.singletonList(
            TablePart.builder().id(tableId).table(tableId).start(ByteArrayUtils.EMPTY_BYTES).build());
    }

    @Override
    public Map<CommonId, Location> mirrors(CommonId tableId) {
        return MetaAdaptorRegistry.getMetaAdaptor(Table.class).get(tableId).getLocations();
    }

}
