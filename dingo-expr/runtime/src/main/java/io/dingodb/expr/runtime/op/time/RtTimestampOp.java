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

package io.dingodb.expr.runtime.op.time;

import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.expr.runtime.op.RtFun;

import java.sql.Date;
import javax.annotation.Nonnull;

public class RtTimestampOp extends RtFun {
    private static final long serialVersionUID = 2173314754701549374L;

    /**
     * Returns the milliseconds elapsed since epoch time (1970-01-01 00:00:00.000+0000) for a para of TIME type.
     *
     * @param paras the parameters of the op
     */
    public RtTimestampOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
        return ((Date) values[0]).getTime();
    }

    @Override
    public int typeCode() {
        return TypeCode.LONG;
    }
}
