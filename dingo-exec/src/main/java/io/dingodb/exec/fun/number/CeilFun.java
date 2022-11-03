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

package io.dingodb.exec.fun.number;

import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.op.RtFun;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Slf4j
public class CeilFun extends RtFun {
    public static final String NAME = "ceil";
    private static final long serialVersionUID = 343548288100628982L;

    public CeilFun(RtExpr[] paras) {
        super(paras);
    }

    public static @NonNull BigDecimal ceiling(final @NonNull BigDecimal value) {
        return value.setScale(0, RoundingMode.CEILING);
    }

    public static @NonNull BigDecimal ceil(final BigDecimal value) {
        return ceiling(value);
    }

    @Override
    protected @Nullable Object fun(Object @NonNull [] values) {
        if (values[0] == null) {
            return null;
        }

        BigDecimal value = new BigDecimal(String.valueOf(values[0]));
        return ceiling(value);
    }

    @Override
    public int typeCode() {
        return TypeCode.DECIMAL;
    }
}
