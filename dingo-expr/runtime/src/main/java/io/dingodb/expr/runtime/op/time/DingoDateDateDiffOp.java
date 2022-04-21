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

import com.google.auto.service.AutoService;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.expr.runtime.op.RtFun;
import io.dingodb.expr.runtime.op.RtOp;
import io.dingodb.expr.runtime.op.time.timeformatmap.DateFormatUtil;
import io.dingodb.func.DingoFuncProvider;

import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
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

        LocalDate fromDate = LocalDateTime.parse(DateFormatUtil.completeToDatetimeFormat(date1),
            DateFormatUtil.getDatetimeFormatter()).toLocalDate();
        LocalDate toDate = LocalDateTime.parse(DateFormatUtil.completeToDatetimeFormat(date0),
            DateFormatUtil.getDatetimeFormatter()).toLocalDate();

        return (toDate.atStartOfDay().atZone(ZoneId.systemDefault()).toEpochSecond()
            - fromDate.atStartOfDay(ZoneId.systemDefault()).toEpochSecond()) /  (24 * 60 * 60);
    }

    public static Long dateDiff(String inputStr1, String inputStr2) {
        // Guarantee the timestamp format.
        if (inputStr1.split(" ").length == 1) {
            inputStr1 += " 00:00:00";
        }
        if (inputStr2.split(" ").length == 1) {
            inputStr2 += " 00:00:00";
        }
        return (Timestamp.valueOf(inputStr1).getTime() - Timestamp.valueOf(inputStr2).getTime())
            / (1000 * 60 * 60 * 24);
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {

        public Function<RtExpr[], RtOp> supplier() {
            return DingoDateDateDiffOp::new;
        }

        @Override
        public String name() {
            return "datediff";
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(DingoDateDateDiffOp.class.getMethod("dateDiff", String.class, String.class));
                return methods;
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
