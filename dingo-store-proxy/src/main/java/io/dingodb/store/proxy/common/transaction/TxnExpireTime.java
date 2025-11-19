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

package io.dingodb.store.proxy.common.transaction;

/**
 * Transaction expiration time tracking
 */
public class TxnExpireTime {
    private boolean initialized;
    private long txnExpire;

    public void update(long lockExpire) {
        if (lockExpire <= 0) {
            lockExpire = 0;
        }
        if (!initialized) {
            txnExpire = lockExpire;
            initialized = true;
        } else if (lockExpire < txnExpire) {
            txnExpire = lockExpire;
        }
    }

    public long getValue() {
        return initialized ? txnExpire : 0;
    }
}
