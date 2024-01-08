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

    public int getTxn_retry_limit() {
        return Integer.parseInt(sessionVariables.getProperty("txn_retry_cnt"));
    }

    public boolean isDisable_txn_auto_retry() {
        return (sessionVariables.getProperty("txn_retry") == "on") ? true : false;
    }

    public boolean isConstraint_check_in_place() {
        return sessionVariables.getProperty("txn_inert_check") == "on" ? true : false;
    }
}
