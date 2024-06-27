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

package io.dingodb.store.api.transaction;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

public interface StoreTxnServiceProvider {
    @Slf4j
    class Impl {
        private static final StoreTxnServiceProvider.Impl INSTANCE = new StoreTxnServiceProvider.Impl();

        private final Map<String, StoreTxnServiceProvider> services = new HashMap<>();
        private final StoreTxnServiceProvider storeTxnServiceProvider;

        private Impl() {
            for (StoreTxnServiceProvider provider : ServiceLoader.load(StoreTxnServiceProvider.class)) {
                services.put(provider.key(), provider);
            }
            this.storeTxnServiceProvider = services.get(DEFAULT);
        }
    }

    static StoreTxnServiceProvider getDefault() {
        return StoreTxnServiceProvider.Impl.INSTANCE.storeTxnServiceProvider;
    }

    static StoreTxnServiceProvider get(String key) {
        return StoreTxnServiceProvider.Impl.INSTANCE.services.get(key);
    }

    String DEFAULT = "default";

    default String key() {
        return DEFAULT;
    }

    StoreTxnService get();
}
