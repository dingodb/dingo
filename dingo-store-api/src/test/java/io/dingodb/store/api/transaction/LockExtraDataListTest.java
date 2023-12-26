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

package io.dingodb.store.api.transaction;

import io.dingodb.common.CommonId;
import io.dingodb.store.api.transaction.data.prewrite.LockExtraDataList;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class LockExtraDataListTest {
    @Test
    void testEncode() {
        CommonId tableId = CommonId.EMPTY_TABLE;
        CommonId partId = CommonId.EMPTY_DISTRIBUTE;
        CommonId txnId = CommonId.EMPTY_TRANSACTION;
        CommonId serverId = CommonId.EMPTY_TXN_INSTANCE;
        int transactionType = 1;
        LockExtraDataList lockExtraDataList = LockExtraDataList.builder()
                .tableId(tableId)
                .partId(partId)
                .txnId(txnId)
                .serverId(serverId)
                .transactionType(transactionType)
                .build();
        byte[] expected = new byte[LockExtraDataList.LEN];
        expected[LockExtraDataList.LEN - 1] = (byte) transactionType;
        System.arraycopy(tableId.encode(), 0, expected, 0, CommonId.LEN);
        System.arraycopy(partId.encode(), 0, expected, CommonId.LEN, CommonId.LEN);
        System.arraycopy(txnId.encode(), 0, expected, CommonId.LEN * 2, CommonId.LEN);
        System.arraycopy(serverId.encode(), 0, expected, CommonId.LEN * 3, CommonId.LEN);
        assertArrayEquals(expected, lockExtraDataList.encode());
    }

    @Test
    void testDecode() {
        CommonId tableId = CommonId.EMPTY_TABLE;
        CommonId partId = CommonId.EMPTY_DISTRIBUTE;
        CommonId txnId = CommonId.EMPTY_TRANSACTION;
        CommonId serverId = CommonId.EMPTY_TXN_INSTANCE;
        int transactionType = 1;
        byte[] content = new byte[LockExtraDataList.LEN];
        System.arraycopy(tableId.encode(), 0, content, 0, CommonId.LEN);
        System.arraycopy(partId.encode(), 0, content, CommonId.LEN, CommonId.LEN);
        System.arraycopy(txnId.encode(), 0, content, CommonId.LEN * 2, CommonId.LEN);
        System.arraycopy(serverId.encode(), 0, content, CommonId.LEN * 3, CommonId.LEN);
        content[LockExtraDataList.LEN - 1] = (byte) transactionType;
        LockExtraDataList expected = LockExtraDataList.builder()
                .tableId(tableId)
                .partId(partId)
                .txnId(txnId)
                .serverId(serverId)
                .transactionType(transactionType)
                .build();
        assertEquals(expected.toString(), LockExtraDataList.decode(content).toString());
    }
}
