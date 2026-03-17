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

package io.dingodb.exec.fun;

import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.BinaryOp;
import io.dingodb.expr.runtime.op.OpKey;
import io.dingodb.expr.runtime.op.OpKeys;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serial;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class DateAddFun extends BinaryOp {
    @Serial
    private static final long serialVersionUID = -1914862455684545159L;

    public static final DateAddFun INSTANCE = new DateAddFun();

    public static final String NAME = "DATE_ADD";

    @Override
    public OpKey keyOf(@NonNull Type type0, @NonNull Type type1) {
        return OpKeys.DATE_LONG.keyOf(type0, type1);
    }

    @Override
    public Object evalValue(@NonNull Object value0, @NonNull Object value1, ExprConfig config) {
        int delta = 0;
        if (value1 instanceof Integer) {
            delta = (int) value1;
        } else if (value1 instanceof Long) {
            Long deltaLong = (Long) value1;
            delta = deltaLong.intValue();
        } else if (value1 instanceof Float) {
            Float deltaFloat = (Float) value1;
            delta = Math.round(deltaFloat);
        } else if (value1 instanceof BigDecimal) {
            BigDecimal decimal = (BigDecimal) value1;
            delta = ((Long) Math.round(decimal.doubleValue())).intValue();
        } else if (value1 instanceof Double) {
            Double deltaDouble = (Double) value1;
            delta = ((Long) Math.round(deltaDouble)).intValue();
        } else if (value1 instanceof Date || value1 instanceof Time || value1 instanceof Timestamp) {
            return null;
        }
        if (value0 instanceof Date) {
            Date date = (Date) value0;
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            calendar.add(Calendar.DAY_OF_MONTH, delta);
            return new Date(calendar.getTimeInMillis());
        } else if (value0 instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) value0;
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(timestamp);
            calendar.add(Calendar.DAY_OF_MONTH, delta);
            return new Date(calendar.getTimeInMillis());
        }
        return value0;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
