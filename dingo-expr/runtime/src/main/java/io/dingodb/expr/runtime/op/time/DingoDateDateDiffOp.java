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

import java.time.LocalDateTime;
import java.time.Period;
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
        String date0 = (String)values[0];
        String date1 = (String)values[1];

        LocalDateTime fromDateTime = LocalDateTime.parse(DateFormatUtil.completeToDatetimeFormat(date0),
            DateFormatUtil.getDatetimeFormatter());
        LocalDateTime toDateTime = LocalDateTime.parse(DateFormatUtil.completeToDatetimeFormat(date1),
            DateFormatUtil.getDatetimeFormatter());
        Period p = Period.between(fromDateTime.toLocalDate(), toDateTime.toLocalDate());
        return p.getDays();

    }
}
