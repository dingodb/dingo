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

package io.dingodb.sdk.operation.executive;

import io.dingodb.common.Executive;
import io.dingodb.common.type.DingoType;
import io.dingodb.sdk.operation.context.Context;
import io.dingodb.sdk.operation.number.ComputeDouble;
import io.dingodb.sdk.operation.number.ComputeFloat;
import io.dingodb.sdk.operation.number.ComputeInteger;
import io.dingodb.sdk.operation.number.ComputeLong;
import io.dingodb.sdk.operation.number.ComputeNumber;

import javax.activation.UnsupportedDataTypeException;

public abstract class AbstractExecutive<D extends Context, T> implements Executive<D, T> {

    public static ComputeNumber convertType(Object value, DingoType dingoType) throws UnsupportedDataTypeException {
        if (dingoType == null) {
            throw new IllegalArgumentException();
        }
        value = dingoType.parse(value);
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

}
