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

package io.dingodb.expr.runtime.op.number;

import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.expr.runtime.op.RtFun;

import java.math.BigDecimal;
import javax.annotation.Nonnull;

public class DingoNumberFormatOp extends RtFun  {
    private static final long serialVersionUID = 4805636716328583550L;

    public DingoNumberFormatOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
        int inputScalar = new BigDecimal(String.valueOf(values[1]))
            .setScale(0, BigDecimal.ROUND_HALF_UP).intValue();
        if (inputScalar < 0) {
            inputScalar = 0;
        }

        BigDecimal decimal = new BigDecimal(String.valueOf(values[0])).setScale(inputScalar, BigDecimal.ROUND_HALF_UP);
        return decimal.toString();
    }

    @Override
    public int typeCode() {
        return TypeCode.DOUBLE;
    }
}
