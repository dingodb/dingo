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

package io.dingodb.server.executor.session;

import io.dingodb.common.session.SessionUtil;
import io.dingodb.driver.DingoConnection;

import java.sql.Connection;
import java.util.Map;

public final class SessionManager {

    private SessionManager() {
    }

    public static void checkOldRunningTxn(Map<Long, Long> jobsVerMap, Map<Long, String> jobsIdsMap) {
        Map<String, Connection> connMap = SessionUtil.INSTANCE.getConnectionMap();

        SessionUtil.INSTANCE.rLock();
        connMap.values().forEach(connection -> {
            DingoConnection dingoConnection = (DingoConnection) connection;
            dingoConnection.removeLockDDLJobs(jobsVerMap, jobsIdsMap);
        });
        SessionUtil.INSTANCE.rUnlock();
    }

}
