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

package io.dingodb.exec.transaction.operator;

import io.dingodb.common.CommonId;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.base.TwoPhaseCommitData;
import io.dingodb.exec.transaction.base.TxnLocalData;
import io.dingodb.exec.transaction.params.RollBackParam;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.transaction.util.TwoPhaseCommitUtils;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;

@Slf4j
public class RollBackOperator extends TransactionOperator {
    public static final RollBackOperator INSTANCE = new RollBackOperator();

    private RollBackOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        synchronized (vertex) {
            RollBackParam param = vertex.getParam();
            TxnLocalData txnLocalData = (TxnLocalData) tuple[0];
            CommonId.CommonType type = txnLocalData.getDataType();
            CommonId txnId = txnLocalData.getTxnId();
            CommonId tableId = txnLocalData.getTableId();
            CommonId newPartId = txnLocalData.getPartId();
            int op = txnLocalData.getOp().getCode();
            byte[] key = txnLocalData.getKey();
            long forUpdateTs = 0;
            boolean isPessimistic = param.getTransactionType() == TransactionType.PESSIMISTIC;
            // first key is primary key
            if (isPessimistic && (ByteArrayUtils.compare(key, param.getPrimaryKey(), 1) == 0)) {
                return true;
            }
            byte[] keyBytes = Arrays.copyOf(key, key.length);
            key = TwoPhaseCommitUtils.commitKey(txnId, tableId, newPartId, op, key, isPessimistic);
            if (key == null) {
                return true;
            }
            forUpdateTs = TwoPhaseCommitUtils.getForUpdateTs(
                txnId,
                tableId,
                newPartId,
                isPessimistic,
                forUpdateTs,
                keyBytes
            );
            CommonId partId = param.getPartId();
            if (partId == null) {
                partId = newPartId;
                param.setPartId(partId);
                param.setTableId(tableId);
                param.addKey(key);
                param.addForUpdateTs(forUpdateTs);
            } else if (partId.equals(newPartId)) {
                param.addKey(key);
                param.addForUpdateTs(forUpdateTs);
                if (param.getKeys().size() == TransactionUtil.max_pre_write_count) {
                    TwoPhaseCommitData twoPhaseCommitData = TwoPhaseCommitData.builder()
                        .primaryKey(param.getPrimaryKey())
                        .isPessimistic(param.isPessimistic())
                        .isolationLevel(param.getIsolationLevel())
                        .txnId(txnId)
                        .type(param.getTransactionType())
                        .build();
                    boolean result = TwoPhaseCommitUtils.txnRollBack(
                        txnId,
                        tableId,
                        partId,
                        param.getKeys(),
                        param.getForUpdateTsList(),
                        twoPhaseCommitData
                    );
                    if (!result) {
                        throw new RuntimeException(txnId + " " + partId + ",txnBatchRollback false");
                    }
                    param.getKeys().clear();
                    param.getForUpdateTsList().clear();
                    param.setPartId(null);
                }
            } else {
                TwoPhaseCommitData twoPhaseCommitData = TwoPhaseCommitData.builder()
                    .primaryKey(param.getPrimaryKey())
                    .isPessimistic(param.isPessimistic())
                    .isolationLevel(param.getIsolationLevel())
                    .txnId(txnId)
                    .type(param.getTransactionType())
                    .build();
                boolean result = TwoPhaseCommitUtils.txnRollBack(
                    txnId,
                    param.getTableId(),
                    partId,
                    param.getKeys(),
                    param.getForUpdateTsList(),
                    twoPhaseCommitData
                );
                if (!result) {
                    throw new RuntimeException(txnId + " " + partId + ",txnBatchRollback false");
                }
                param.getKeys().clear();
                param.addKey(key);
                param.getForUpdateTsList().clear();
                param.addForUpdateTs(forUpdateTs);
                param.setPartId(newPartId);
                param.setTableId(tableId);
            }
            return true;
        }
    }

    @Override
    public void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        synchronized (vertex) {
            if (!(fin instanceof FinWithException)) {
                RollBackParam param = vertex.getParam();
                if (!param.getKeys().isEmpty()) {
                    CommonId txnId = vertex.getTask().getTxnId();
                    TwoPhaseCommitData twoPhaseCommitData = TwoPhaseCommitData.builder()
                        .primaryKey(param.getPrimaryKey())
                        .isPessimistic(param.isPessimistic())
                        .isolationLevel(param.getIsolationLevel())
                        .txnId(txnId)
                        .type(param.getTransactionType())
                        .build();
                    boolean result = TwoPhaseCommitUtils.txnRollBack(
                        txnId,
                        param.getTableId(),
                        param.getPartId(),
                        param.getKeys(),
                        param.getForUpdateTsList(),
                        twoPhaseCommitData
                    );
                    if (!result) {
                        throw new RuntimeException(txnId + " " + param.getPartId() + ",txnBatchRollback false");
                    }
                    param.getKeys().clear();
                }
                vertex.getSoleEdge().transformToNext(new Object[]{true});
            }
            vertex.getSoleEdge().fin(fin);
        }
    }

}
