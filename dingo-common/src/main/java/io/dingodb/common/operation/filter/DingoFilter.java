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

package io.dingodb.common.operation.filter;

import io.dingodb.common.operation.context.OperationContext;
import io.dingodb.common.store.KeyValue;

import java.io.IOException;
import java.util.Arrays;

public interface DingoFilter {

    boolean filter(OperationContext context, KeyValue keyValue);

    void addOrFilter(DingoFilter filter);

    void addAndFilter(DingoFilter filter);

    default int[] getKeyIndex(OperationContext context, int[] indexes) {
        return Arrays.stream(indexes)
            .filter(i -> context.definition.getColumn(i).isPrimary())
            .toArray();
    }

    default int[] getValueIndex(OperationContext context, int[] indexes) {
        return Arrays.stream(indexes)
            .filter(i -> !context.definition.getColumn(i).isPrimary())
            .map(i -> i - context.definition.getPrimaryKeyCount())
            .toArray();
    }

    default Object[] getRecord(
        int[] keyIndex,
        int[] valueIndex,
        KeyValue keyValue,
        OperationContext context) throws IOException {

        Object[] recordKey;
        Object[] recordValue;
        if (keyIndex.length > 0 && valueIndex.length > 0) {
            recordKey = context.dingoKeyCodec().decode(keyValue.getKey(), keyIndex);
            recordValue = context.dingoValueCodec().decode(keyValue.getValue(), valueIndex);
            Object[] record0 = new Object[keyIndex.length + valueIndex.length];
            System.arraycopy(recordKey, 0, record0, 0, recordKey.length);
            System.arraycopy(recordValue, 0, record0, recordKey.length, recordValue.length);
            return record0;
        }
        if (keyIndex.length > 0) {
            recordKey = context.dingoKeyCodec().decode(keyValue.getKey(), keyIndex);
            return recordKey;
        }
        if (valueIndex.length > 0) {
            recordValue = context.dingoValueCodec().decode(keyValue.getValue(), valueIndex);
            return recordValue;
        }
        return null;
    }
}
