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

package io.dingodb.common.operation.executive;

import io.dingodb.common.operation.compute.number.ComputeDouble;
import io.dingodb.common.operation.compute.number.ComputeFloat;
import io.dingodb.common.operation.compute.number.ComputeInteger;
import io.dingodb.common.operation.compute.number.ComputeLong;
import io.dingodb.common.operation.compute.number.ComputeNumber;
import io.dingodb.common.operation.context.OperationContext;
import io.dingodb.common.store.KeyValue;

import javax.activation.UnsupportedDataTypeException;
import java.util.Iterator;

public abstract class NumberExecutive<D extends OperationContext, T extends Iterator<KeyValue>, R>
    implements Executive<D, T, R> {

    public static ComputeNumber convertType(Object value) throws UnsupportedDataTypeException {
        if (value instanceof Integer) {
            return new ComputeInteger((Integer) value);
        } else if (value instanceof Long) {
            return new ComputeLong((Long) value);
        } else if (value instanceof Double) {
            return new ComputeDouble((Double) value);
        } else if (value instanceof Float) {
            return new ComputeFloat((Float) value);
        } else {
            throw new UnsupportedDataTypeException(value.toString());
        }
    }

    public static byte[] arrayCopy(KeyValue keyValue) {
        byte[] bytes = new byte[keyValue.getKey().length + keyValue.getValue().length];
        System.arraycopy(keyValue.getKey(), 0, bytes, 0, keyValue.getKey().length);
        System.arraycopy(keyValue.getValue(), 0, bytes, keyValue.getKey().length, keyValue.getValue().length);
        return bytes;
    }

}
