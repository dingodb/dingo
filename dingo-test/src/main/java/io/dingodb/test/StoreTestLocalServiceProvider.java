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

package io.dingodb.test;

import com.google.auto.service.AutoService;
import io.dingodb.store.api.StoreService;
import io.dingodb.store.api.StoreServiceProvider;
import io.dingodb.store.memory.MemoryStoreService;

@AutoService(StoreServiceProvider.class)
public class StoreTestLocalServiceProvider implements StoreServiceProvider {

    public static final MemoryStoreService STORE_SERVICE = new MemoryStoreService();

    @Override
    public StoreService get() {
        return STORE_SERVICE;
    }

    @Override
    public String key() {
        return "local";
    }
}
