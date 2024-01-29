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
import io.dingodb.exec.Services;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.fin.Fin;
import io.dingodb.exec.fin.FinWithException;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.transaction.params.CommitParam;
import io.dingodb.exec.transaction.util.TransactionUtil;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.api.transaction.data.commit.TxnCommit;
import io.dingodb.store.api.transaction.exception.ReginSplitException;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
public class CommitOperator extends TransactionOperator {
    public static final CommitOperator INSTANCE = new CommitOperator();

    private CommitOperator() {
    }

    @Override
    public synchronized boolean push(Context context, @Nullable Object[] tuple, Vertex vertex) {
        CommitParam param = vertex.getParam();
        CommonId.CommonType type = CommonId.CommonType.of((byte) tuple[0]);
        CommonId txnId = (CommonId) tuple[1];
        CommonId tableId = (CommonId) tuple[2];
        CommonId newPartId = (CommonId) tuple[3];
        int op = (byte) tuple[4];
        byte[] key = (byte[]) tuple[5];
        if (ByteArrayUtils.compare(key, param.getPrimaryKey(), 9) == 0) {
            return true;
        }
        if (tableId.type == CommonId.CommonType.INDEX) {
            IndexTable indexTable = TransactionUtil.getIndexDefinitions(tableId);
            if (indexTable.indexType.isVector) {
                key = Arrays.copyOf(key, key.length -4);
            }
        }
        CommonId partId = param.getPartId();
        if (partId == null) {
            partId = newPartId;
            param.setPartId(partId);
            param.setTableId(tableId);
            param.addKey(key);
        } else if (partId.equals(newPartId)) {
            param.addKey(key);
            if (param.getKeys().size() == TransactionUtil.max_pre_write_count) {
                boolean result = txnCommit(param, txnId, tableId, partId);
                if (!result) {
                    throw new RuntimeException(txnId + " " + partId + ",txnCommit false,PrimaryKey:" + param.getPrimaryKey().toString());
                }
                param.getKeys().clear();
                param.setPartId(null);
            }
        } else {
            boolean result = txnCommit(param, txnId, tableId, partId);
            if (!result) {
                throw new RuntimeException(txnId + " " + partId + ",txnCommit false,PrimaryKey:" + param.getPrimaryKey().toString());
            }
            param.getKeys().clear();
            param.addKey(key);
            param.setPartId(newPartId);
            param.setTableId(tableId);
        }
        return true;
    }

    private boolean txnCommit(CommitParam param, CommonId txnId, CommonId tableId, CommonId newPartId) {
        // 1、Async call sdk TxnCommit
        TxnCommit commitRequest = TxnCommit.builder()
            .isolationLevel(IsolationLevel.of(param.getIsolationLevel()))
            .startTs(param.getStartTs())
            .commitTs(param.getCommitTs())
            .keys(param.getKeys())
            .build();
        try {
            StoreInstance store = Services.KV_STORE.getInstance(tableId, newPartId);
            return store.txnCommit(commitRequest);
        } catch (ReginSplitException e) {
            log.error(e.getMessage(), e);
            // 2、regin split
            Map<CommonId, List<byte[]>> partMap = TransactionUtil.multiKeySplitRegionId(tableId, txnId, param.getKeys());
            for (Map.Entry<CommonId, List<byte[]>> entry : partMap.entrySet()) {
                CommonId regionId = entry.getKey();
                List<byte[]> value = entry.getValue();
                StoreInstance store = Services.KV_STORE.getInstance(tableId, regionId);
                commitRequest.setKeys(value);
                boolean result = store.txnCommit(commitRequest);
                if (!result) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public synchronized void fin(int pin, @Nullable Fin fin, Vertex vertex) {
        CommitParam param = vertex.getParam();
        if (!(fin instanceof FinWithException)) {
            if (param.getKeys().size() > 0) {
                CommonId txnId = vertex.getTask().getTxnId();
                boolean result = txnCommit(param, txnId, param.getTableId(), param.getPartId());
                if (!result) {
                    throw new RuntimeException(txnId + " " + param.getPartId() + ",txnCommit false,PrimaryKey:" + param.getPrimaryKey().toString());
                }
            }
            vertex.getSoleEdge().transformToNext(new Object[]{true});
        }
        vertex.getSoleEdge().fin(fin);
    }

}
