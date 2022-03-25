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

import java.sql.Timestamp;
import javax.annotation.Nonnull;

public class DingoDateDateDiffOp extends RtFun {

    public DingoDateDateDiffOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Override
    public int typeCode() {
        return TypeCode.LONG;
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
        String date1 = (String)values[0];
        String date2 = (String)values[1];
        // Guarantee the timestamp format.
        if (date1.split(" ").length == 1) {
            date1 += " 00:00:00";
        }
        if (date2.split(" ").length == 1) {
            date2 += " 00:00:00";
        }
        return (Timestamp.valueOf(date1).getTime() - Timestamp.valueOf(date2).getTime()) / (1000 * 60 * 60 * 24);
    }
}
