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

package io.dingodb.server.executor.schedule;

import io.dingodb.common.concurrent.Executors;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.tenant.TenantConstant;
import io.dingodb.sdk.service.LockService;
import io.dingodb.store.proxy.Configuration;
import io.dingodb.transaction.api.GcService;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public final class SafePointUpdateTask {
    public static volatile boolean isLeader;
    private static final String lockKeyStr =  "safe_point_update_" + TenantConstant.TENANT_ID;
    private static ScheduledFuture<?> future;
    private static ScheduledFuture<?> regionDelFuture;

    public static final SafePointUpdateTask safePointUpdateTask = new SafePointUpdateTask();

    private SafePointUpdateTask() {
    }

    public static void runScheduleSafePointUpdate() {
        isLeader = true;
        LogUtils.info(log, "Start safe point update task.");
        future = Executors.scheduleWithFixedDelay(
            lockKeyStr, SafePointUpdateTask::safePointUpdate, 1, 300, TimeUnit.SECONDS
        );
        regionDelFuture = Executors.scheduleWithFixedDelay(
            lockKeyStr, SafePointUpdateTask::gcDeleteRegion, 60, 60, TimeUnit.SECONDS
        );
    }

    public static void cancelScheduleSafePointUpdate() {
        if (future != null) {
            try {
                future.cancel(true);
            } catch (Exception e) {
                LogUtils.error(log, e.getMessage(), e);
            }
        }
        if (regionDelFuture != null) {
            try {
                regionDelFuture.cancel(true);
            } catch (Exception e) {
                LogUtils.error(log, e.getMessage(), e);
            }
        }
        isLeader = false;
    }

    private static void safePointUpdate() {
        GcService.getDefault().safePointUpdate();
    }

    private static void gcDeleteRegion() {
        synchronized (safePointUpdateTask) {
            GcService.getDefault().gcDeleteRegion();
        }
    }

}
