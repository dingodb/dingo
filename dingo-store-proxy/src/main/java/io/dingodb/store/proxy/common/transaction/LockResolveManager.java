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

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class LockResolveManager {
    private LockResolveManager() {
    }

    private static final Map<Long, TxnStatus> resolvedTxns = new ConcurrentHashMap<>();

    private static final LinkedHashSet<Long> recentResolved = new LinkedHashSet<>();

    private static final int RESOLVED_CACHE_SIZE = 1024;


    public static TxnStatus getResolved(long txnID) {
        return resolvedTxns.get(txnID);
    }

    public static void saveResolved(long txnID, TxnStatus status) {
        resolvedTxns.put(txnID, status);
        recentResolved.add(txnID);

        if (recentResolved.size() > RESOLVED_CACHE_SIZE) {
            Long oldest = recentResolved.iterator().next();
            recentResolved.remove(oldest);
            resolvedTxns.remove(oldest);
        }
    }

}
