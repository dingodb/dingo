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

package io.dingodb.server.executor.ddl;

import io.dingodb.common.ddl.DdlJob;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.session.Session;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ReorgUtil {

    private ReorgUtil() {
    }

    public static void cleanupDDLReorgHandles(DdlJob job) {
        if (job != null && !job.isFinished() && !job.isSynced()) {
            return;
        }
        try {
            JobTableUtil.cleanDDLReorgHandles(job);
        } catch (Exception e) {
            LogUtils.warn(log, "Failed removing the DDL reorg entry in dingo_ddl_reorg");
        }
    }

}
