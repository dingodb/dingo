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

package io.dingodb.common.type;

import io.dingodb.expr.core.TypeCode;
import io.dingodb.serial.schema.BooleanListSchema;
import io.dingodb.serial.schema.BytesListSchema;
import io.dingodb.serial.schema.DingoSchema;
import io.dingodb.serial.schema.DoubleListSchema;
import io.dingodb.serial.schema.FloatListSchema;
import io.dingodb.serial.schema.IntegerListSchema;
import io.dingodb.serial.schema.LongListSchema;
import io.dingodb.serial.schema.StringListSchema;

public final class TypeUtils {
    private TypeUtils() {
    }

    public static DingoSchema elementTypeToDingoList(DingoType elementType) {
        switch (elementType.getTypeCode()) {
            case TypeCode.BOOL:
                return new BooleanListSchema(-1);
            case TypeCode.INT:
                return new IntegerListSchema(-1);
            case TypeCode.FLOAT:
                return new FloatListSchema(-1);
            case TypeCode.DOUBLE:
                return new DoubleListSchema(-1);
            case TypeCode.STRING:
                return new StringListSchema(-1);
            case TypeCode.TIME:
            case TypeCode.TIMESTAMP:
            case TypeCode.DATE:
            case TypeCode.LONG:
                return new LongListSchema(-1);
            case TypeCode.BINARY:
            case TypeCode.OBJECT:
            case TypeCode.DECIMAL:
                return new BytesListSchema(-1);
            default:
                throw new RuntimeException("Not Support ListType");
        }
    }
}
