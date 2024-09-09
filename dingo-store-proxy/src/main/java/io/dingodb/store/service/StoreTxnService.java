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

package io.dingodb.store.service;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.store.api.transaction.StoreKvTxn;

public class StoreTxnService implements io.dingodb.store.api.transaction.StoreTxnService {
    public static final StoreTxnService DEFAULT_INSTANCE = new StoreTxnService();

    @AutoService(io.dingodb.store.api.transaction.StoreTxnServiceProvider.class)
    public static final class StoreKvTxnServiceProvider
        implements io.dingodb.store.api.transaction.StoreTxnServiceProvider {
        @Override
        public io.dingodb.store.api.transaction.StoreTxnService get() {
            return DEFAULT_INSTANCE;
        }
    }

    @Override
    public StoreKvTxn getInstance(CommonId tableId, CommonId regionId) {
        return new io.dingodb.store.service.StoreKvTxn(tableId, regionId);
    }
}
