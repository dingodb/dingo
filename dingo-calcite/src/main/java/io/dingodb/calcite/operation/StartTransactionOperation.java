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

package io.dingodb.calcite.operation;

import io.dingodb.transaction.api.TransactionService;

import java.sql.Connection;
import java.sql.SQLException;

public class StartTransactionOperation implements DdlOperation {

    private Connection connection;

    private boolean pessimistic;

    public StartTransactionOperation(Connection connection, boolean pessimistic) {
        this.connection = connection;
        this.pessimistic = pessimistic;
    }

    @Override
    public void execute() {
        try {
            TransactionService.getDefault().begin(connection, pessimistic);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
