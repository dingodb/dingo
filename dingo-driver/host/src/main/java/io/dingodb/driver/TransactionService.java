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

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.meta.entity.Table;
import io.dingodb.transaction.api.LockType;
import io.dingodb.transaction.api.TableLock;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class TransactionService implements io.dingodb.transaction.api.TransactionService {

    public static final TransactionService DEFAULT_INSTANCE = new TransactionService();

    @Override
    public void begin(Connection connection, boolean pessimistic) throws SQLException {
        if (connection instanceof DingoConnection) {
            ((DingoConnection) connection).beginTransaction(pessimistic);
        }
    }

    @Override
    public void commit(Connection connection) throws SQLException {
        connection.commit();
    }

    @Override
    public void rollback(Connection connection) throws SQLException {
        connection.rollback();
    }

    @Override
    public void lockTable(Connection connection, List<CommonId> tables, LockType type) {
        ((DingoConnection) connection).lockTables(tables, type);
    }

    @Override
    public void unlockTable(Connection connection) {
        ((DingoConnection) connection).unlockTables();
    }

    @AutoService(io.dingodb.transaction.api.TransactionServiceProvider.class)
    public static final class TransactionServiceProvider implements io.dingodb.transaction.api.TransactionServiceProvider {

        @Override
        public TransactionService get() {
            return DEFAULT_INSTANCE;
        }
    }


}
