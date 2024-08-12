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
import io.dingodb.meta.entity.InfoCache;
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
                mdlCheckTableInfo.wLock();
                long maxVer = mdlCheckTableInfo.getNewestVer();
                if (maxVer > saveMaxSchemaVersion) {
                    saveMaxSchemaVersion = maxVer;
                } else if (!jobNeedToSync) {
                    mdlCheckTableInfo.wUnlock();
                    //LogUtils.info(log, "[ddl] mdl check not need to sync,max ver:{} saveMaxSchema ver:{}", maxVer, saveMaxSchemaVersion);
                    continue;
                }
                int jobNeedToCheckCnt = mdlCheckTableInfo.getJobsVerMap().size();
                if (jobNeedToCheckCnt == 0) {
                    jobNeedToSync = false;
                    mdlCheckTableInfo.wUnlock();
                    LogUtils.info(log, "[ddl] mdl check job need to check cnt is 0,max ver:{} saveMaxSchema ver:{}", maxVer, saveMaxSchemaVersion);
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

                for (Map.Entry<Long, Long> entry : jobsVerMap.entrySet()) {
                    if (jobCache.containsKey(entry.getKey())
                        && jobCache.get(entry.getKey()) >= entry.getValue()
                        && DdlContext.INSTANCE.getNewVer().get() >= entry.getValue()) {
                        continue;
                    }
                    LogUtils.info(log, "mdl gets lock, update to owner, jobId:{}, version:{}", entry.getKey(), entry.getValue());
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
