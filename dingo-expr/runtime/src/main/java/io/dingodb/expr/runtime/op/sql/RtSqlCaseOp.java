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

package io.dingodb.expr.runtime.op.sql;

import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.expr.runtime.op.RtFun;

import javax.annotation.Nonnull;

public final class RtSqlCaseOp extends RtFun {
    private static final long serialVersionUID = 262253285071682317L;

    public RtSqlCaseOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Override
    public int typeCode() {
        return TypeCode.OBJECT;
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
        int size = values.length;
        for (int i = 0; i < size - 1; i += 2) {
            if ((boolean) values[i]) {
                return values[i + 1];
            }
        }
        // There will be a `null` if you missed `ELSE` in SQL.
        return values[size - 1];
    }
}
