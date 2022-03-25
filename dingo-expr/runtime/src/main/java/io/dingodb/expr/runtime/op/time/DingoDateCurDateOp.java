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

import javax.annotation.Nonnull;

/**
 * Create an DingoDteNowOp to process date.
 */

public class DingoDateCurDateOp extends RtFun {

    public static final long serialVersionUID = 5420406898012966400L;


    public DingoDateCurDateOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Override
    public int typeCode() {
        return TypeCode.DATE;
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
        return new java.sql.Date(System.currentTimeMillis());
    }
}
