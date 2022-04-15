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

package io.dingodb.server.executor.api;

import io.dingodb.common.CommonId;
import io.dingodb.net.NetService;
import io.dingodb.store.api.Part;
import io.dingodb.store.api.StoreService;

public class TableStoreApi implements io.dingodb.server.api.TableStoreApi {

    private final StoreService storeService;

    public TableStoreApi(NetService netService, StoreService storeService) {
        this.storeService = storeService;
        netService.apiRegistry().register(io.dingodb.server.api.TableStoreApi.class, this);
    }

    @Override
    public void newTable(CommonId id) {
        storeService.getInstance(id);
    }

    @Override
    public void deleteTable(CommonId id) {
        storeService.deleteInstance(id);
    }

    @Override
    public void newTablePart(Part part) {
        storeService.getInstance(part.getInstanceId()).assignPart(part);
    }

    @Override
    public void removeTablePart(Part part) {
        storeService.getInstance(part.getInstanceId()).unassignPart(part);
    }

    @Override
    public void deleteTablePart(Part part) {
        storeService.getInstance(part.getInstanceId()).deletePart(part);
    }
}
