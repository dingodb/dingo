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

package io.dingodb.store.service;

import io.dingodb.common.codec.CodecKvUtil;
import io.dingodb.tso.TsoService;

import java.util.List;

public class TxStructure {
    MetaStoreKvTxn txn;
    MetaStoreKvTxn ddlTxn;
    private final Long startTs;

    public TxStructure(long startTs) {
        this.startTs = startTs;
        this.txn = MetaStoreKvTxn.getInstance();
        this.ddlTxn = MetaStoreKvTxn.getDdlInstance();
    }

    public long getLong(byte[] key) {
        byte[] val = get(key);
        if (val == null) {
            return 0;
        }
        return Long.parseLong(new String(val));
    }

    public byte[] get(byte[] key) {
        byte[] ek = CodecKvUtil.encodeStringDataKey(key);
        long realStartTs = startTs != 0L ? startTs : TsoService.getDefault().tso();
        return txn.mGet(ek, realStartTs);
    }

    public byte[] ddlGet(byte[] key) {
        byte[] ek = CodecKvUtil.encodeStringDataKey(key);
        long realStartTs = startTs != 0L ? startTs : TsoService.getDefault().tso();
        return ddlTxn.mGetImmediately(ek, realStartTs);
    }

    public List<byte[]> hGetAll(byte[] key) {
        byte[] dataPrefix = CodecKvUtil.hashDataKeyPrefix(key);
        byte[] end = CodecKvUtil.hashDataKeyPrefixUpperBound(dataPrefix);
        long realStartTs = startTs != 0L ? startTs : TsoService.getDefault().tso();
        return txn.mRange(dataPrefix, end, realStartTs);
    }

    public synchronized Long inc(byte[] key, long step) {
        byte[] ek = CodecKvUtil.encodeStringDataKey(key);
        long realStartTs = startTs != 0L ? startTs : TsoService.getDefault().tso();
        byte[] val = txn.mGet(ek, realStartTs);
        long id = 0L;
        if (val != null) {
            id = Long.parseLong(new String(val));;
        }
        id += step;

        String idStr = String.valueOf(id);
        realStartTs = startTs != 0L ? startTs : TsoService.getDefault().tso();
        txn.put(ek, idStr.getBytes(), realStartTs);
        return id;
    }

    public void put(byte[] key, byte[] value) {
        byte[] ek = CodecKvUtil.encodeStringDataKey(key);
        long realStartTs = startTs != 0L ? startTs : TsoService.getDefault().tso();
        txn.put(ek, value, realStartTs);
    }

    public void ddlPut(byte[] key, byte[] value) {
        byte[] ek = CodecKvUtil.encodeStringDataKey(key);
        long realStartTs = startTs != 0L ? startTs : TsoService.getDefault().tso();
        ddlTxn.put(ek, value, realStartTs);
    }

    public void hInsert(byte[] key, byte[] field, byte[] value) {
        byte[] dataKey = CodecKvUtil.encodeHashDataKey(key, field);
        long realStartTs = startTs != 0L ? startTs : TsoService.getDefault().tso();
        this.txn.mInsert(dataKey, value, realStartTs);
    }

    public byte[] hGet(byte[] key, byte[] field) {
        byte[] dataKey = CodecKvUtil.encodeHashDataKey(key, field);
        long realStartTs = startTs != 0L ? startTs : TsoService.getDefault().tso();
        return this.txn.mGet(dataKey, realStartTs);
    }

    public byte[] ddlHGet(byte[] key, byte[] field) {
        byte[] dataKey = CodecKvUtil.encodeHashDataKey(key, field);
        long realStartTs = startTs != 0L ? startTs : TsoService.getDefault().tso();
        return this.ddlTxn.mGetImmediately(dataKey, realStartTs);
    }

    public void hDel(byte[] key, byte[] field) {
        byte[] dataKey = CodecKvUtil.encodeHashDataKey(key, field);
        long realStartTs = startTs != 0L ? startTs : TsoService.getDefault().tso();
        this.txn.mDel(dataKey, realStartTs);
    }

    public void hPut(byte[] prefix, byte[] data, byte[] value) {
        byte[] dataKey = CodecKvUtil.encodeHashDataKey(prefix, data);
        long realStartTs = startTs != 0L ? startTs : TsoService.getDefault().tso();
        this.txn.put(dataKey, value, realStartTs);
    }

    public void ddlHPut(byte[] prefix, byte[] data, byte[] value) {
        byte[] dataKey = CodecKvUtil.encodeHashDataKey(prefix, data);
        long realStartTs = startTs != 0L ? startTs : TsoService.getDefault().tso();
        this.ddlTxn.put(dataKey, value, realStartTs);
    }

    public List<byte[]> mRange(byte[] start, byte[] end) {
        long realStartTs = startTs != 0L ? startTs : TsoService.getDefault().tso();
        return this.txn.mRange(start, end, realStartTs);
    }

}
