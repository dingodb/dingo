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

import io.dingodb.sdk.service.entity.store.Action;
import io.dingodb.sdk.service.entity.store.LockInfo;
import io.dingodb.store.api.transaction.data.resolvelock.ResolveLockStatus;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TxnStatus {
    private long ttl;
    private long commitTs;
    private Action action;
    private LockInfo primaryLock;
    @Builder.Default
    private ResolveLockStatus resolveLockStatus = ResolveLockStatus.NONE;

    public boolean isCommitted() {
        return ttl == 0 && commitTs > 0;
    }

    public boolean isRolledBack() {
        return ttl == 0 && commitTs == 0;
    }

    public boolean isStatusCacheable() {
        if (isCommitted()) return true;
        if (ttl == 0) {
            return
                action == Action.LockNotExistRollback ||
                action == Action.TTLExpireRollback;
        }
        return false;
    }
}
