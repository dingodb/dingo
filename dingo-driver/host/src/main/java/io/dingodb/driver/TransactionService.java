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
import io.dingodb.common.session.SessionUtil;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.transaction.api.LockType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
    public void rollback(long txnId) throws SQLException {
        Connection connection1 = SessionUtil.INSTANCE.getConnectionMap()
            .values().stream()
            .filter(connection -> {
                DingoConnection dingoConn = (DingoConnection) connection;
                return dingoConn.getTransaction() != null && dingoConn.getTransaction().getTxnId() != null
                     && dingoConn.getTransaction().getTxnId().seq == txnId;
            }).findFirst()
            .orElse(null);
        if (connection1 != null) {
            connection1.rollback();
        }
    }

    @Override
    public void lockTable(Connection connection, List<CommonId> tables, LockType type) {
        ((DingoConnection) connection).lockTables(tables, type);
    }

    @Override
    public void unlockTable(Connection connection) {
        ((DingoConnection) connection).unlockTables();
    }

    @Override
    public Iterator<Object[]> getMdlInfo() {
        Map<String, Connection> connectionMap = SessionUtil.INSTANCE.getConnectionMap();
        return connectionMap.values().stream()
            .map(conn -> (DingoConnection)conn)
            .filter(dc -> !dc.getMdlLockJobMap().isEmpty())
            .filter(dc -> dc.getTransaction() != null)
            .map(dc -> {
                long jobId = dc.getMdlLockJobMap().keySet().stream().findFirst().orElse(0L);
                ITransaction transaction = dc.getTransaction();
                List<String> sqlList = dc.getTransaction().getSqlList();
                Object[] res = new Object[3];
                res[0] = jobId;
                res[1] = transaction.getStartTs();
                StringBuilder sqlBuilder = new StringBuilder();
                for (String sql : sqlList) {
                    sqlBuilder.append(sql).append(";");
                }
                res[2] = sqlBuilder.toString();
                return res;
            })
            .iterator();
    }

    /**
     * Get the information of running transaction.
     * @return
     */
    @Override
    public Iterator<Object[]> getTxnInfo() {
        Map<String, Connection> connectionMap = SessionUtil.INSTANCE.getConnectionMap();
        return connectionMap.values().stream()
            .map(conn -> (DingoConnection)conn)
            .filter(dc -> !dc.getMdlLockJobMap().isEmpty())
            .filter(dc -> dc.getTransaction() != null)
            .map(dc -> {
                long jobId = dc.getMdlLockJobMap().keySet().stream().findFirst().orElse(0L);
                ITransaction transaction = dc.getTransaction();
                List<String> sqlList = dc.getTransaction().getSqlList();
                Object[] res = new Object[14];

                //Get transaction id as string.
                res[0] = transaction.getTxnId().toString();

                //Get transaction type as string.
                res[1] = transaction.getType().toString();

                //Get job id as long.
                res[2] = jobId;

                //Get transaction start timestamp as long.
                res[3] = transaction.getStartTs();

                //Get transaction commit timestamp as long.
                res[4] = transaction.getCommitTs();

                //Get sql list in transaction as string.
                StringBuilder sqlBuilder = new StringBuilder();
                for (String sql : sqlList) {
                    sqlBuilder.append(sql).append(";");
                }
                res[5] = sqlBuilder.toString();

                //Get transaction status.
                res[6] = transaction.getStatus().toString();

                //Get transaction isolation level.
                res[7] = IsolationLevel.of(transaction.getIsolationLevel()).toString();

                //Get transaction auto commit flag.
                res[8] = String.valueOf(transaction.isAutoCommit());

                //Get transaction primary key.
                try {
                    res[9] = (transaction.getPrimaryKey() != null) ? transaction.getPrimaryKey().toString() : "";
                } catch (UnsupportedOperationException e) {
                    res[9] = "";
                }

                //Get forUpdateTs.
                try {
                    res[10] = transaction.getForUpdateTs();
                } catch (UnsupportedOperationException e) {
                    res[10] = 0;
                }

                //Get lock timeout time.
                res[11] = transaction.getLockTimeOut();

                //Get primary key lock.
                try {
                    if (transaction.getPrimaryKeyLock() == null) {
                        res[12] = "";
                    } else {
                        StringBuilder hexString = new StringBuilder();
                        hexString.append("0X");
                        for (byte b : transaction.getPrimaryKeyLock()) {
                            hexString.append(String.format("%02X", b));
                        }
                        res[12] = hexString;
                    }
                } catch(UnsupportedOperationException e) {
                    res[12] = "";
                }

                //Get Job Seq id.
                try {
                    res[13] = String.valueOf(transaction.getJobSeqId());
                } catch (UnsupportedOperationException e) {
                    res[13] = "";
                }
                return res;
            })
            .iterator();
    }

    @AutoService(io.dingodb.transaction.api.TransactionServiceProvider.class)
    public static final class TransactionServiceProvider implements io.dingodb.transaction.api.TransactionServiceProvider {

        @Override
        public TransactionService get() {
            return DEFAULT_INSTANCE;
        }
    }


}
