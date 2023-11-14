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

package io.dingodb.calcite.type.converter;

import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.converter.DataConverter;
import io.dingodb.expr.runtime.ExprCompiler;
import io.dingodb.expr.runtime.expr.Exprs;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.util.NlsString;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class RexLiteralConverter implements DataConverter {
    public static final RexLiteralConverter INSTANCE = new RexLiteralConverter();
    private static final RuntimeException NEVER_CONVERT_BACK
        = new IllegalStateException("Convert back to RexLiteral should be avoided.");

    private RexLiteralConverter() {
    }

    @Override
    public Object convert(@NonNull Date value) {
        throw NEVER_CONVERT_BACK;
    }

    @Override
    public Object convert(@NonNull Time value) {
        throw NEVER_CONVERT_BACK;
    }

    @Override
    public Object convert(@NonNull Timestamp value) {
        throw NEVER_CONVERT_BACK;
    }

    @Override
    public Object convert(byte @NonNull [] value) {
        throw NEVER_CONVERT_BACK;
    }

    @Override
    public Object convert(Object @NonNull [] value, @NonNull DingoType elementType) {
        throw NEVER_CONVERT_BACK;
    }

    @Override
    public Integer convertIntegerFrom(@NonNull Object value) {
        return (Integer) ExprCompiler.ADVANCED.visit(Exprs.op(Exprs.TO_INT_C, Exprs.val(value))).eval();
    }

    @Override
    public Long convertLongFrom(@NonNull Object value) {
        return (Long) ExprCompiler.ADVANCED.visit(Exprs.op(Exprs.TO_LONG_C, Exprs.val(value))).eval();
    }

    @Override
    public Float convertFloatFrom(@NonNull Object value) {
        return ((BigDecimal) value).floatValue();
    }

    @Override
    public Double convertDoubleFrom(@NonNull Object value) {
        return ((BigDecimal) value).doubleValue();
    }

    @Override
    public String convertStringFrom(@NonNull Object value) {
        return ((NlsString) value).getValue();
    }

    @Override
    public Date convertDateFrom(@NonNull Object value) {
        return new Date(((Calendar) value).getTimeInMillis());
    }

    @Override
    public Time convertTimeFrom(@NonNull Object value) {
        return new Time(((Calendar) value).getTimeInMillis());
    }

    @Override
    public Timestamp convertTimestampFrom(@NonNull Object value) {
        // This works for literal like `TIMESTAMP '1970-01-01 00:00:00'`, which returns UTC time, not local time
        Calendar calendar = (Calendar) value;
        long v = calendar.getTimeInMillis();
        return new Timestamp(v - calendar.getTimeZone().getOffset(v));
    }

    @Override
    public byte[] convertBinaryFrom(@NonNull Object value) {
        return ((ByteString) value).getBytes();
    }
}
