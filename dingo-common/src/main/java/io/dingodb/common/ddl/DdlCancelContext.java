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

package io.dingodb.common.ddl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public final class DdlCancelContext {
    private static final ReentrantLock lock = new ReentrantLock();
    private static final List<String> cancelConIdList = new ArrayList<>();

    private DdlCancelContext() {
    }

    public static boolean cancel(String connId) {
        lock.lock();
        try {
            boolean cancelFlg = cancelConIdList.contains(connId);
            if (cancelFlg) {
                cancelConIdList.remove(connId);
            }
            return cancelFlg;
        } finally {
            lock.unlock();
        }
    }

    public static void addCancel(String connId) {
        try {
            lock.lock();
            cancelConIdList.add(connId);
        } finally {
            lock.unlock();
        }
    }
}
