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

package io.dingodb.driver;

import io.dingodb.common.CommonId;
import io.dingodb.exec.base.JobManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.Meta;

@Slf4j
public final class DingoStatementUtils {
    private DingoStatementUtils() {
    }

    public static void removeJobInSignature(JobManager jobManager, Meta.Signature signature) {
        if (signature instanceof DingoSignature) {
            CommonId jobId = ((DingoSignature) signature).getJobId();
            if (jobId != null) {
                if (log.isDebugEnabled()) {
                    log.debug("Job id \"{}\" found in signature, remove it.", jobId);
                }
                jobManager.removeJob(jobId);
            }
        }
    }
}
