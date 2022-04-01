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

package io.dingodb.expr.runtime.op.string;

import io.dingodb.expr.runtime.RtExpr;

import java.math.BigDecimal;
import java.util.Collections;
import javax.annotation.Nonnull;

public class DingoStringRepeatOp extends RtStringConversionOp {
    private static final long serialVersionUID = 7673054922107009329L;

    /**
     * Create an DingoStringRepeatOp. repeat the String times.
     *
     * @param paras the parameters of the op
     */
    public DingoStringRepeatOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Nonnull
    @Override
    protected Object fun(@Nonnull Object[] values) {
        String inputStr = ((String)values[0]).trim();
        int times = new BigDecimal(String.valueOf(values[1])).setScale(0, BigDecimal.ROUND_HALF_UP).intValue();

        if (times < 0) {
            return "";
        }
        return String.join("", Collections.nCopies(times, inputStr));
    }
}
