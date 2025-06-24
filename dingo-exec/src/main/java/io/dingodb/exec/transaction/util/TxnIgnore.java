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

package io.dingodb.exec.transaction.util;

import com.codahale.metrics.Timer;
import io.dingodb.common.CommonId;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.metrics.DingoMetrics;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Pair;
import io.dingodb.exec.Services;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.base.TxnLocalData;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.exec.transaction.params.CommitParam;
import io.dingodb.exec.transaction.params.PreWriteParam;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Mutation;
import io.dingodb.store.api.transaction.exception.DuplicateEntryException;
import io.dingodb.store.api.transaction.exception.WriteConflictException;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.dingodb.common.CommonId.CommonType.LOAD_DATA;
import static io.dingodb.exec.transaction.util.TransactionUtil.max_pre_write_count;

@Slf4j
public class TxnIgnore extends Txn {
    private byte[] txnIdKey;

    public TxnIgnore(CommonId txnId, boolean retry, int retryCnt, long timeOut) {
        super(txnId, retry, retryCnt, timeOut);
        this.txnIdKey = txnId.encode();
    }

    public int commit(List<TxnLocalData> tupleList) {
        List<TxnLocalData> secondList = preWritePrimary(tupleList);
        if (!secondList.isEmpty()) {
            preWriteSecondSkipError(secondList);
        }
        try {
            this.commitTs = TransactionManager.getCommitTs();
            if (primaryObj == null) {
                return 0;
            }
            // commit primary key
            boolean result = commitPrimaryData(primaryObj);
            if (!result) {
                assert primaryObj != null;
                throw new RuntimeException(txnId + " " + primaryObj.getPartId()
                    + ",txnCommitPrimaryKey false,commit_ts:" + commitTs + ",PrimaryKey:"
                    + Arrays.toString(primaryKey));
            }
            // commit second key
            commitSecond();
            return tupleList.size();
        } finally {
            if (future != null) {
                future.cancel(true);
            }
        }
    }

    private List<TxnLocalData> preWritePrimary(List<TxnLocalData> tupleList) {
        // get local mem data first data and transform to cacheToObject
        int successIndex = -1;
        for (int i = 0; i < tupleList.size(); i ++) {
            try {
                TxnLocalData primary = tupleList.get(i);
                primaryObj = getCacheToObject(primary);
                preWritePrimaryKey(primaryObj);
                successIndex = i + 1;
                break;
            } catch (WriteConflictException | DuplicateEntryException e) {
                LogUtils.info(log, e.getMessage(), e);
                primaryObj = null;
            }
        }
        if (successIndex > -1 && successIndex <= tupleList.size()) {
            return tupleList.subList(successIndex, tupleList.size());
        } else {
            return new ArrayList<>();
        }
    }

    protected void preWriteSecondSkipError(List<TxnLocalData> secondList) {
        try {
            Timer.Context timeCtx = DingoMetrics.getTimeContext("ReorgPreSecond");
            preWriteSecondKey(secondList);
            timeCtx.stop();
        } catch (WriteConflictException e) {
            if (e.doneCnt > 0) {
                secondList = secondList.subList(e.doneCnt, secondList.size());
            }
            secondList.removeIf(txnLocalData -> {
                byte[] conflictKey = txnLocalData.getKey();
                return ByteArrayUtils.compare(e.key, conflictKey) == 0;
            });
            preWriteSecondSkipError(secondList);
        } catch (DuplicateEntryException e1) {
            if (e1.doneCnt > 0) {
                secondList = secondList.subList(e1.doneCnt, secondList.size());
            }
            if (e1.keys == null) {
                throw e1;
            }
            secondList.removeIf(txnLocalData -> {
                byte[] conflictKey = txnLocalData.getKey();
                return e1.keys.stream().anyMatch(key -> ByteArrayUtils.compare(key, conflictKey, 9) == 0);
            });
            preWriteSecondSkipError(secondList);
        }
    }

    protected void preWriteSecondKey(List<TxnLocalData> secondList) {
        PreWriteParam param = new PreWriteParam(dingoType, primaryKey, startTs,
            isolationLevel, TransactionType.OPTIMISTIC, timeOut);
        param.init(null);
        int preDoneCnt = 0;
        try {
            for (TxnLocalData txnLocalData : secondList) {
                CommonId txnId = txnLocalData.getTxnId();
                CommonId tableId = txnLocalData.getTableId();
                CommonId newPartId = txnLocalData.getPartId();
                int op = txnLocalData.getOp().getCode();
                byte[] key = txnLocalData.getKey();
                byte[] value = txnLocalData.getValue();
                Mutation mutation = TransactionCacheToMutation.cacheToMutation(
                    op, key, value, 0L, tableId, newPartId, txnId
                );
                CommonId partId = param.getPartId();
                if (partId == null) {
                    partId = newPartId;
                    param.setPartId(partId);
                    param.setTableId(tableId);
                    param.addMutation(mutation);
                } else if (partId.equals(newPartId)) {
                    param.addMutation(mutation);
                    if (param.getMutations().size() == TransactionUtil.max_pre_write_count) {
                        Pair<Boolean, Map<CommonId, List<byte[]>>> result
                            = txnPreWriteWithRePartId(param, txnId, tableId, partId);
                        if (!result.getKey()) {
                            throw new RuntimeException(txnId + " " + partId + ",txnPreWrite false,PrimaryKey:"
                                + Arrays.toString(param.getPrimaryKey()));
                        }
                        preDoneCnt += param.getMutations().size();
                        result.getValue().forEach((key1, value1) -> this.removeDoneKey(key1, value1, tableId));
                        param.getMutations().clear();
                        param.setPartId(null);
                    }
                } else {
                    Pair<Boolean, Map<CommonId, List<byte[]>>> result
                        = txnPreWriteWithRePartId(param, txnId, param.getTableId(), partId);
                    if (!result.getKey()) {
                        throw new RuntimeException(txnId + " " + partId + ",txnPreWrite false,PrimaryKey:"
                            + Arrays.toString(param.getPrimaryKey()));
                    }
                    preDoneCnt += param.getMutations().size();
                    result.getValue().forEach((key1, value1) -> this.removeDoneKey(key1, value1, tableId));
                    param.getMutations().clear();
                    param.addMutation(mutation);
                    param.setPartId(newPartId);
                    param.setTableId(tableId);
                }
            }
        } catch (WriteConflictException e) {
            e.doneCnt += preDoneCnt;
            throw e;
        } catch (DuplicateEntryException e1) {
            e1.doneCnt += preDoneCnt;
            throw e1;
        }

        if (!param.getMutations().isEmpty()) {
            try {
                Pair<Boolean, Map<CommonId, List<byte[]>>> result
                    = txnPreWriteWithRePartId(param, txnId, param.getTableId(), param.getPartId());
                if (!result.getKey()) {
                    throw new RuntimeException(txnId + " " + param.getPartId() + ",txnPreWrite false,PrimaryKey:"
                        + Arrays.toString(param.getPrimaryKey()));
                }
                preDoneCnt += param.getMutations().size();
                result.getValue().forEach((key1, value1) -> this.removeDoneKey(key1, value1, param.getTableId()));
                param.getMutations().clear();
            } catch (WriteConflictException e) {
                e.doneCnt += preDoneCnt;
                throw e;
            } catch (DuplicateEntryException e1) {
                e1.doneCnt += preDoneCnt;
                throw e1;
            }
        }
    }

    private void removeDoneKey(CommonId part, List<byte[]> mutationList, CommonId tableId) {
        Timer.Context timeCtx = DingoMetrics.getTimeContext("removeDoneKey");
        StoreInstance localStore = Services.LOCAL_STORE.getInstance(null, null);
        byte[] tableIdKey = tableId.encode();
        mutationList.forEach(key -> {
            byte[] partId = part.encode();
            byte[] localKey = getLocalKey(key, partId, tableIdKey);
            localStore.put(new KeyValue(localKey, null));
        });
        timeCtx.stop();
    }

    protected byte[] getLocalKey(byte[] key, byte[] partId, byte[] tableIdKey) {
        byte[] ek = new byte[key.length + 1 + tableIdKey.length + partId.length + txnIdKey.length];
        ek[0] = (byte) LOAD_DATA.getCode();
        System.arraycopy(txnIdKey, 0, ek, 1, txnIdKey.length);
        System.arraycopy(tableIdKey, 0, ek, 1 + txnIdKey.length, tableIdKey.length);
        System.arraycopy(partId, 0, ek, 1 + txnIdKey.length + tableIdKey.length, partId.length);
        System.arraycopy(key, 0, ek, 1 + txnIdKey.length + partId.length + tableIdKey.length, key.length);
        return ek;
    }

    public boolean commitSecond() {
        if (primaryObj == null) {
            return true;
        }
        Iterator<KeyValue> iterator = getLocalIterator();

        CommitParam param = new CommitParam(dingoType, isolationLevel, txnId.seq,
            commitTs, primaryKey, TransactionType.OPTIMISTIC);
        param.init(null);
        while (iterator.hasNext()) {
            KeyValue keyValue = iterator.next();
            int from = 1;
            //Arrays.copyOfRange(keyValue.getKey(), from, from += CommonId.LEN);
            from += CommonId.LEN;
            CommonId tableId = CommonId.decode(Arrays.copyOfRange(keyValue.getKey(), from, from += CommonId.LEN));
            CommonId newPartId = CommonId.decode(Arrays.copyOfRange(keyValue.getKey(), from, from += CommonId.LEN));
            byte[] key = new byte[keyValue.getKey().length - from];
            System.arraycopy(keyValue.getKey(), from , key, 0, key.length);
            CommonId partId = param.getPartId();
            if (partId == null) {
                partId = newPartId;
                param.setPartId(partId);
                param.setTableId(tableId);
                param.addKey(key);
            } else if (partId.equals(newPartId)) {
                param.addKey(key);
                if (param.getKeys().size() == max_pre_write_count) {
                    boolean result = txnCommit(param, txnId, tableId, partId);
                    if (!result) {
                        throw new RuntimeException(txnId + " " + partId + ",txnCommit false,PrimaryKey:"
                            + Arrays.toString(param.getPrimaryKey()));
                    }
                    param.getKeys().clear();
                    param.setPartId(null);
                }
            } else {
                boolean result = txnCommit(param, txnId, param.getTableId(), partId);
                if (!result) {
                    throw new RuntimeException(txnId + " " + partId + ",txnCommit false,PrimaryKey:"
                        + Arrays.toString(param.getPrimaryKey()));
                }
                param.getKeys().clear();
                param.addKey(key);
                param.setPartId(newPartId);
                param.setTableId(tableId);
            }
        }
        if (!param.getKeys().isEmpty()) {
            boolean result = txnCommit(param, txnId, param.getTableId(), param.getPartId());
            if (!result) {
                throw new RuntimeException(txnId + " " + param.getPartId() + ",txnCommit false,PrimaryKey:"
                    + Arrays.toString(param.getPrimaryKey()));
            }
        }
        return true;
    }

    private Iterator<KeyValue> getLocalIterator() {
        StoreInstance cache = Services.LOCAL_STORE.getInstance(null, null);
        byte[] prefix = new byte[1 + txnIdKey.length];
        prefix[0] = (byte) LOAD_DATA.getCode();
        System.arraycopy(txnIdKey, 0, prefix, 1, txnIdKey.length);
        return cache.scan(prefix);
    }
}
