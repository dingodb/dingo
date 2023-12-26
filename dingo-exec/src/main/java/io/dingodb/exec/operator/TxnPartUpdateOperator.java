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

package io.dingodb.exec.operator;

import io.dingodb.common.CommonId;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.Services;
import io.dingodb.exec.converter.ValueConverter;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.exec.operator.params.TxnPartUpdateParam;
import io.dingodb.exec.utils.ByteUtils;
import io.dingodb.partition.DingoPartitionServiceProvider;
import io.dingodb.partition.PartitionService;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.api.transaction.data.Op;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.dingodb.common.util.NoBreakFunctions.wrap;

@Slf4j
public class TxnPartUpdateOperator extends PartModifyOperator {
    public static final TxnPartUpdateOperator INSTANCE = new TxnPartUpdateOperator();

    private TxnPartUpdateOperator() {
    }

    @Override
    protected boolean pushTuple(Object[] tuple, Vertex vertex) {
        TxnPartUpdateParam param = vertex.getParam();
        DingoType schema = param.getSchema();
        TupleMapping mapping = param.getMapping();
        List<SqlExpr> updates = param.getUpdates();

        int tupleSize = schema.fieldCount();
        Object[] newTuple = Arrays.copyOf(tuple, tupleSize);
        boolean updated = false;
        int i = 0;
        try {
            for (i = 0; i < mapping.size(); ++i) {
                Object newValue = updates.get(i).eval(tuple);
                int index = mapping.get(i);
                if ((newTuple[index] == null && newValue != null)
                    || (newTuple[index] != null && !newTuple[index].equals(newValue))
                ) {
                    newTuple[index] = newValue;
                    updated = true;
                }
            }
            Object[] newTuple2 = (Object[]) schema.convertFrom(newTuple, ValueConverter.INSTANCE);
            KeyValue keyValue = wrap(param.getCodec()::encode).apply(newTuple2);
            CommonId tableId = param.getTableId();
            CommonId partId = PartitionService.getService(
                    Optional.ofNullable(param.getTableDefinition().getPartDefinition())
                        .map(PartitionDefinition::getFuncName)
                        .orElse(DingoPartitionServiceProvider.RANGE_FUNC_NAME))
                .calcPartId(keyValue.getKey(), param.getDistributions());
            StoreInstance store = Services.LOCAL_STORE.getInstance(tableId, partId);
            byte[] txnIdBytes = vertex.getTask().getTxnId().encode();
            byte[] tableIdBytes = tableId.encode();
            byte[] partIdBytes = partId.encode();
            keyValue.setKey(ByteUtils.encode(
                keyValue.getKey(),
                Op.PUT.getCode(),
                (txnIdBytes.length + tableIdBytes.length + partIdBytes.length),
                txnIdBytes, tableIdBytes, partIdBytes));
            byte[] insertKey = Arrays.copyOf(keyValue.getKey(), keyValue.getKey().length);
            insertKey[insertKey.length - 2] = (byte) Op.PUTIFABSENT.getCode();
            store.delete(insertKey);
            store.delete(keyValue.getKey());
            updated = updated && store.put(keyValue);
            if (updated) {
                param.inc();
            }
        } catch (Exception ex) {
            log.error("txn update operator with expr:{}, exception:{}",
                updates.get(i) == null ? "None" : updates.get(i).getExprString(),
                ex, ex);
            throw new RuntimeException("Txn_update Operator catch Exception");
        }
        return true;
    }
}
