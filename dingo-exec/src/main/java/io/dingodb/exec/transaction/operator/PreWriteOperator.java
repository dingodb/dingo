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
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.transaction.base.TwoPhaseCommitData;
import io.dingodb.exec.transaction.base.TxnLocalData;
import io.dingodb.exec.transaction.params.PreWriteParam;
import io.dingodb.exec.transaction.util.TransactionCacheToMutation;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.exec.transaction.util.TwoPhaseCommitUtils;
import io.dingodb.store.api.transaction.data.Mutation;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public final class PreWriteOperator extends TransactionOperator {
    public static final PreWriteOperator INSTANCE = new PreWriteOperator();

    private PreWriteOperator() {
    }

    @Override
    public boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        synchronized (vertex) {
            PreWriteParam param = vertex.getParam();
            TxnLocalData txnLocalData = (TxnLocalData) tuple[0];
            CommonId.CommonType type = txnLocalData.getDataType();
            CommonId txnId = txnLocalData.getTxnId();
            CommonId tableId = txnLocalData.getTableId();
            CommonId newPartId = txnLocalData.getPartId();
            int op = txnLocalData.getOp().getCode();
            byte[] key = txnLocalData.getKey();
            byte[] value = txnLocalData.getValue();
            // first key is primary key
            if (ByteArrayUtils.compare(key, param.getPrimaryKey(), 1) == 0) {
                return true;
            }
            Mutation mutation = TransactionCacheToMutation.preWriteMutation(
                txnId,
                tableId,
                newPartId,
                op,
                key,
                value,
                param.isPessimistic()
            );
            LogUtils.debug(log, "mutation: {}", mutation);
            CommonId partId = param.getPartId();
            if (partId == null) {
                partId = newPartId;
                param.setPartId(partId);
                param.setTableId(tableId);
                param.addMutation(mutation);
            } else if (partId.equals(newPartId)) {
                param.addMutation(mutation);
                if (param.getMutations().size() == TransactionUtil.max_pre_write_count) {
                    TwoPhaseCommitData twoPhaseCommitData = TwoPhaseCommitData.builder()
                        .primaryKey(param.getPrimaryKey())
                        .isPessimistic(param.isPessimistic())
                        .isolationLevel(param.getIsolationLevel())
                        .txnId(vertex.getTask().getTxnId())
                        .type(param.getTransactionType())
                        .lockTimeOut(param.getTimeOut())
                        .build();
                    boolean result = TwoPhaseCommitUtils.txnPreWrite(
                        tableId,
                        partId,
                        param.getMutations(),
                        twoPhaseCommitData
                    );
                    if (!result) {
                        throw new RuntimeException(txnId + " " + partId
                            + ",txnPreWrite false,PrimaryKey:" + param.getPrimaryKey().toString());
                    }
                    param.getMutations().clear();
                    param.setPartId(null);
                }
            } else {
                TwoPhaseCommitData twoPhaseCommitData = TwoPhaseCommitData.builder()
                    .primaryKey(param.getPrimaryKey())
                    .isPessimistic(param.isPessimistic())
                    .isolationLevel(param.getIsolationLevel())
                    .txnId(vertex.getTask().getTxnId())
                    .type(param.getTransactionType())
                    .lockTimeOut(param.getTimeOut())
                    .build();
                boolean result = TwoPhaseCommitUtils.txnPreWrite(
                    param.getTableId(),
                    partId,
                    param.getMutations(),
                    twoPhaseCommitData
                );
                if (!result) {
                    throw new RuntimeException(txnId + " " + partId
                        + ",txnPreWrite false,PrimaryKey:" + param.getPrimaryKey().toString());
                }
                param.getMutations().clear();
                param.addMutation(mutation);
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
                PreWriteParam param = vertex.getParam();
                if (param.getMutations().size() > 0) {
                    TwoPhaseCommitData twoPhaseCommitData = TwoPhaseCommitData.builder()
                        .primaryKey(param.getPrimaryKey())
                        .isPessimistic(param.isPessimistic())
                        .isolationLevel(param.getIsolationLevel())
                        .txnId(vertex.getTask().getTxnId())
                        .type(param.getTransactionType())
                        .lockTimeOut(param.getTimeOut())
                        .build();
                    boolean result = TwoPhaseCommitUtils.txnPreWrite(
                        param.getTableId(),
                        param.getPartId(),
                        param.getMutations(),
                        twoPhaseCommitData
                    );
                    if (!result) {
                        throw new RuntimeException(vertex.getTask().getTxnId() + " " + param.getPartId()
                            + ",txnPreWrite false,PrimaryKey:" + param.getPrimaryKey().toString());
                    }
                    param.getMutations().clear();
                }
                vertex.getSoleEdge().transformToNext(new Object[]{true});
            }
            vertex.getSoleEdge().fin(fin);
        }
    }

}
