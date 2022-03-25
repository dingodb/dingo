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
import io.dingodb.expr.runtime.op.time.timeformatmap.DateFormatUtil;

import javax.annotation.Nonnull;

public class DingoDateFromUnixTimeOp extends RtFun {
    public DingoDateFromUnixTimeOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Override
    public int typeCode() {
        return TypeCode.TIMESTAMP;
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
        // Ignore format
        // Todo format need be used in DingoMeta to format the date.
        Long timestamp = 0L;
        try {
            timestamp = (Long) values[0];
        } catch (Exception e) {
            timestamp = ((Integer) values[0]).longValue();
        }
        return new java.sql.Timestamp(timestamp * 1000);
    }
}
