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

import io.dingodb.common.operation.Column;
import io.dingodb.common.operation.compute.number.ComputeDouble;
import io.dingodb.common.operation.compute.number.ComputeFloat;
import io.dingodb.common.operation.compute.number.ComputeInteger;
import io.dingodb.common.operation.compute.number.ComputeLong;
import io.dingodb.common.operation.compute.number.ComputeNumber;
import io.dingodb.common.operation.context.OperationContext;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import javax.activation.UnsupportedDataTypeException;

public abstract class NumberExecutive<D extends OperationContext, T extends Iterator<KeyValue>, R>
    implements Executive<D, T, R> {

    public static ComputeNumber convertType(Object value, DingoType dingoType) throws UnsupportedDataTypeException {
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

    protected DingoType getDingoType(D context) {
        int[] mappings = Arrays.stream(context.columns)
            .map(c -> context.definition.getColumnIndex(c.name))
            .mapToInt(Integer::valueOf)
            .toArray();

        return DingoTypeFactory.tuple(TupleMapping.of(mappings).stream()
            .mapToObj(context.definition.getColumns()::get)
            .map(ColumnDefinition::getDingoType)
            .toArray(DingoType[]::new));
    }

    protected static int[] getKeyIndex(OperationContext context) {
        Column[] columns = context.columns;
        return Arrays.stream(columns)
            .filter(col -> Objects.requireNonNull(context.definition.getColumn(col.name)).isPrimary())
            .map(col -> context.definition.getColumnIndex(col.name)).mapToInt(Integer::valueOf).toArray();
    }

    protected static int[] getValueIndex(OperationContext context) {
        Column[] columns = context.columns;
        return Arrays.stream(columns)
            .filter(col -> !Objects.requireNonNull(context.definition.getColumn(col.name)).isPrimary())
            .map(col -> context.definition.getColumnIndexOfValue(col.name)).mapToInt(Integer::valueOf).toArray();
    }

}
