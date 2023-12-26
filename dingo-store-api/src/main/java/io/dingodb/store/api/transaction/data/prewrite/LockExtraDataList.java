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

package io.dingodb.store.api.transaction.data.prewrite;

import io.dingodb.common.CommonId;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@Builder
@ToString
public class LockExtraDataList {
    private CommonId tableId;
    private CommonId partId;
    private CommonId txnId;
    private CommonId serverId;
    // OPTIMISTIC(0), PESSIMISTIC(1)
    private int transactionType;
    public static final int TYPE_LEN = 1;
    public static final int TABLE_LEN = CommonId.LEN;
    public static final int PART_LEN = CommonId.LEN;
    public static final int TXN_LEN = CommonId.LEN;
    public static final int SERVER_LEN = CommonId.LEN;
    public static final int LEN = TYPE_LEN + TABLE_LEN + PART_LEN + TXN_LEN + SERVER_LEN;
    public byte[] encode() {
        byte[] content = new byte[LEN];
        content[LEN - 1] = (byte) transactionType;
        byte[] tableIdEncode = tableId.encode();
        System.arraycopy(tableIdEncode, 0, content, 0, TABLE_LEN);
        byte[] partIdEncode = partId.encode();
        System.arraycopy(partIdEncode, 0, content, TABLE_LEN, PART_LEN);
        byte[] txnIdEncode = txnId.encode();
        System.arraycopy(txnIdEncode, 0, content, TABLE_LEN + PART_LEN, TXN_LEN);
        byte[] serverIdEncode = serverId.encode();
        System.arraycopy(serverIdEncode, 0, content, TABLE_LEN + PART_LEN + TXN_LEN, SERVER_LEN);
        return content;
    }

   public static LockExtraDataList decode(byte[] content) {
       CommonId tableId = CommonId.decode(content, 0);
       CommonId partId = CommonId.decode(content, TABLE_LEN);
       CommonId txnId = CommonId.decode(content, TABLE_LEN + PART_LEN);
       CommonId serverId = CommonId.decode(content, TABLE_LEN + PART_LEN + TXN_LEN);
       int transactionType = content[content.length - 1];
       return LockExtraDataList.builder()
           .tableId(tableId)
           .partId(partId)
           .txnId(txnId)
           .serverId(serverId)
           .transactionType(transactionType)
           .build();
   }

}
