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

package io.dingodb.common.log;

import org.slf4j.MDC;

public class MdcUtils {
    private MdcUtils() {}

    private static final String STATEMENT_ID= "stmtId";
    private static final String JOB_ID = "jobId";

    private static final String TXN_ID = "txnId";

    public static void setStmtId(String stmtId) {
        MDC.put(STATEMENT_ID, stmtId);
    }

    public static void setJobId(String jobId) {
        MDC.put(JOB_ID, jobId);
    }

    public static void setTxnId(String txnId) {
        MDC.put(TXN_ID, txnId);
    }

    public static String getStmtId() {
        return MDC.get(STATEMENT_ID);
    }

    public static String getJobId() {
        return MDC.get(JOB_ID);
    }

    public static String getTxnId() {
        return MDC.get(TXN_ID);
    }

    public static void removeStmtId() {
        MDC.remove(STATEMENT_ID);
    }

    public static void removeJobId() {
        MDC.remove(JOB_ID);
    }

    public static void removeTxnId() {
        MDC.remove(TXN_ID);
    }

    public static void clear() {
        MDC.clear();
    }
}
