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

import io.dingodb.common.ddl.DdlUtil;
import io.dingodb.common.ddl.MdlCheckTableInfo;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.util.Utils;
import io.dingodb.server.executor.ddl.DdlContext;
import io.dingodb.server.executor.session.SessionManager;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public final class MetaLockCheckHandler {

    private MetaLockCheckHandler() {
    }

    public static void mdlCheckLoop() {
        long saveMaxSchemaVersion = 0;
        boolean jobNeedToSync = false;
        Map<Long, Long> jobCache = new HashMap<>();

        while (!Thread.interrupted()) {
            if (!DdlUtil.mdlEnable) {
                continue;
            }
            try {
                Utils.sleep(50);
                MdlCheckTableInfo mdlCheckTableInfo = ExecutionEnvironment.INSTANCE.mdlCheckTableInfo;
                if (DdlUtil.timeOutError.get()) {
                    LogUtils.info(log, "mdl check continue, save ver:{}", saveMaxSchemaVersion);
                }
                mdlCheckTableInfo.wLock();
                long maxVer = mdlCheckTableInfo.getNewestVer();
                if (DdlUtil.timeOutError.get()) {
                    LogUtils.info(log, "mdl check maxVer:{}, save ver:{}, jobNeedToSync:{}", maxVer, saveMaxSchemaVersion, jobNeedToSync);
                }
                if (maxVer > saveMaxSchemaVersion) {
                    saveMaxSchemaVersion = maxVer;
                } else if (!jobNeedToSync) {
                    mdlCheckTableInfo.wUnlock();
                    if (DdlUtil.timeOutError.get()) {
                        LogUtils.info(log, "[ddl] mdl check not need to sync,max ver:{} saveMaxSchema ver:{}", maxVer, saveMaxSchemaVersion);
                        //DdlUtil.timeOutError.set(false);
                    }
                    continue;
                }
                int jobNeedToCheckCnt = mdlCheckTableInfo.getJobsVerMap().size();
                if (jobNeedToCheckCnt == 0) {
                    jobNeedToSync = false;
                    LogUtils.info(log, "[ddl] mdl check job need to check cnt is 0,max ver:{} saveMaxSchema ver:{}", maxVer, saveMaxSchemaVersion);
                    mdlCheckTableInfo.wUnlock();
                    continue;
                }
                Map<Long, String> jobsIdsMap = new HashMap<>(mdlCheckTableInfo.getJobsIdsMap());
                Map<Long, Long> jobsVerMap = new HashMap<>(mdlCheckTableInfo.getJobsVerMap());
                mdlCheckTableInfo.wUnlock();
                jobNeedToSync = true;
                SessionManager.checkOldRunningTxn(jobsVerMap, jobsIdsMap);
                if (jobsVerMap.size() == jobNeedToCheckCnt) {
                    jobNeedToSync = false;
                }
                if (jobCache.size() > 1000) {
                    jobCache = new HashMap<>();
                }
                if (DdlUtil.timeOutError.get()) {
                    LogUtils.info(log, "[ddl] mdl check jobs id map size:{}, "
                        + "jobs ver map size:{}, job cache:{}, jobNeedToSync:{}", jobsIdsMap.size(),
                        jobsVerMap.size(), jobCache, jobNeedToSync);
                }

                if (jobsVerMap.isEmpty()) {
                    if (DdlUtil.timeOutError.get()) {
                        LogUtils.info(log, "[ddl] mdl check jobs ver map is empty, save ver:{}, max ver:{}", saveMaxSchemaVersion, maxVer);
                        //DdlUtil.timeOutError.set(false);
                    }
                    continue;
                }
                for (Map.Entry<Long, Long> entry : jobsVerMap.entrySet()) {
                    if (jobCache.containsKey(entry.getKey())
                        && jobCache.get(entry.getKey()) >= entry.getValue()
                    ) {
                        if (DdlUtil.timeOutError.get()) {
                            LogUtils.info(log, "[ddl] mdl check skip, max ver:{},"
                             +" saveMaxSchema ver:{}, new ver:{}, jobs ver:{}", maxVer,
                              saveMaxSchemaVersion, DdlContext.INSTANCE.getNewVer(), entry.getValue());
                            //DdlUtil.timeOutError.set(false);
                        }
                        continue;
                    }
                    LogUtils.info(log, "mdl gets lock, update to owner, jobId:{}, version:{}, save ver:{}",
                        entry.getKey(), entry.getValue(), saveMaxSchemaVersion);
                    try {
                        DdlContext.INSTANCE.getSchemaSyncer().updateSelfVersion(System.identityHashCode(entry), entry.getKey(), entry.getValue());
                        jobCache.put(entry.getKey(), entry.getValue());
                    } catch (Exception e) {
                        LogUtils.warn(log, "update self version failed, reason:{}", e.getMessage());
                    }
                }
            } catch (Exception e) {
                LogUtils.error(log, "mdlCheckLoop error, error:" + e.getMessage());
            }
        }
    }
}
