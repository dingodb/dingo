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

package io.dingodb.expr.runtime.op.logical;

import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.expr.runtime.op.RtOp;

import java.math.BigDecimal;
import java.math.BigInteger;
import javax.annotation.Nonnull;

public abstract class RtLogicalOp extends RtOp {
    private static final long serialVersionUID = 5800304351907769891L;

    protected RtLogicalOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    public static boolean test(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof Number) {
            if (value instanceof BigDecimal) {
                throw new RuntimeException("Invalid input parameter.");
            } else if (value instanceof BigInteger) {
                if (((BigInteger) value).compareTo(BigInteger.ZERO) < 0) {
                    throw new RuntimeException("Invalid input parameter.");
                }
                return ((BigInteger) value).compareTo(BigInteger.ZERO) != 0;
            }

            // `Double` is enough to contain integer, long, float, etc.
            if (((Number) value).doubleValue() < 0) {
                throw new RuntimeException("Invalid input parameter.");
            }
            return ((Number) value).doubleValue() != 0.0;
        }

        throw new RuntimeException("Invalid input parameter.");
    }

    @Override
    public final int typeCode() {
        return TypeCode.BOOLEAN;
    }
}
