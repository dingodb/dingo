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

package io.dingodb.transaction.api;

import io.dingodb.common.CommonId;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public interface TransactionService {
    static TransactionService getDefault() {
        return TransactionServiceProvider.getDefault().get();
    }

    void begin(Connection connection, boolean pessimistic) throws SQLException;
    void commit(Connection connection) throws SQLException;
    void rollback(Connection connection) throws SQLException;
    default void rollback(long txnId) throws SQLException {}

    void lockTable(Connection connection, List<CommonId> locks, LockType type);
    void unlockTable(Connection connection);

    Iterator<Object[]> getMdlInfo();
    Iterator<Object[]> getTxnInfo();
}
