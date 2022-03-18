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

import javax.annotation.Nonnull;

public class DingoStringLeftOp extends RtStringConversionOp {
    private static final long serialVersionUID = 5242457055774200528L;

    /**
     * Create an DingoStringLeftOp. DingoStringLeftOp extract left sub string.
     *
     * @param paras the parameters of the op
     */
    public DingoStringLeftOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Nonnull
    @Override
    protected Object fun(@Nonnull Object[] values) {
        Integer cnt = ((Long)(values[1])).intValue();
        return ((String) values[0]).substring(0, cnt);
    }
}
