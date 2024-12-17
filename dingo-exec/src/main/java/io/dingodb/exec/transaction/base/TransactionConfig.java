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

package io.dingodb.exec.transaction.base;

import io.dingodb.common.util.Optional;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Properties;

@NoArgsConstructor
public class TransactionConfig {

    @Setter
    private Properties sessionVariables;

    public TransactionConfig(Properties sessionVariables) {
        this.sessionVariables = sessionVariables;
    }

    public int getTxnRetryLimit() {
        Optional<String> retryCountOpt = Optional.ofNullable(
            sessionVariables.getProperty("txn_retry_cnt"));
        return retryCountOpt
            .map(Integer::parseInt)
            .orElse(0);
    }

    public boolean isDisableTxnAutoRetry() {
        return "on".equalsIgnoreCase(sessionVariables.getProperty("txn_retry"));
    }

    public boolean isConstraintCheckInPlace() {
        return "on".equalsIgnoreCase(sessionVariables.getProperty("txn_inert_check"));
    }

    public long getLockTimeOut() {
        Optional<String> retryCountOpt = Optional.ofNullable(
            sessionVariables.getProperty("lock_wait_timeout"));
        return (retryCountOpt
            .map(Integer::parseInt)
            .orElse(50)) * 1000L;
    }

    public boolean isAsyncCommit() {
        return "on".equalsIgnoreCase(sessionVariables.getProperty("dingo_enable_async_commit"));
    }

    public boolean isCrossNodeCommit() {
        return "on".equalsIgnoreCase(sessionVariables.getProperty("enable_use_cross_node_commit"));
    }
}
