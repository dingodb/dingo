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

import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.RtExpr;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RtAndOp extends RtLogicalOp {
    private static final long serialVersionUID = 5283729329444724953L;

    /**
     * Create an RtAndOp. RtAndOp performs logical AND operation.
     *
     * @param paras the parameters of the op
     */
    public RtAndOp(RtExpr[] paras) {
        super(paras);
    }

    @Override
    public @Nullable Object eval(EvalContext etx) {
        Boolean result = Boolean.TRUE;
        for (RtExpr para : paras) {
            Object v = para.eval(etx);
            if (v == null) {
                if (result == Boolean.TRUE) {
                    result = null;
                }
            } else if (!RtLogicalOp.test(v)) {
                result = Boolean.FALSE;
                break;
            }
        }
        return result;
    }
}
