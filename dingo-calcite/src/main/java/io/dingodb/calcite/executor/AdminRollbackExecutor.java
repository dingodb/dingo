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

package io.dingodb.calcite.executor;

import io.dingodb.common.log.LogUtils;
import io.dingodb.transaction.api.TransactionService;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;

@Slf4j
public class AdminRollbackExecutor implements DdlExecutor {
    long txnId;

    public AdminRollbackExecutor(long txnId) {
        this.txnId = txnId;
    }

    @Override
    public void execute() {
        try {
            TransactionService.getDefault().rollback(txnId);
        } catch (SQLException e) {
            LogUtils.error(log, e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
