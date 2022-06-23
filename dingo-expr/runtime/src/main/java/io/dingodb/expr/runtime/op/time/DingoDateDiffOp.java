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
import io.dingodb.expr.runtime.exception.FailParseTime;
import io.dingodb.expr.runtime.op.RtFun;
import io.dingodb.expr.runtime.op.RtOp;
import io.dingodb.expr.runtime.op.time.utils.DingoDateTimeUtils;
import io.dingodb.func.DingoFuncProvider;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;

@Slf4j
public class DingoDateDiffOp extends RtFun {

    private static Long DAY_MILLI_SECONDS = Long.valueOf(24 * 60 * 60 * 1000);

    public DingoDateDiffOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Override
    public int typeCode() {
        return TypeCode.LONG;
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
        return dateDiff((String)values[0], (String) values[1]);
    }

    public static Long dateDiff(String value0, String value1) {
        if (value0.isEmpty() || value1.isEmpty()) {
            return  null;
        }
        try {
            LocalDateTime time0 = DingoDateTimeUtils.convertToDatetime(value0);
            LocalDateTime time1 = DingoDateTimeUtils.convertToDatetime(value1);
            return time0.toLocalDate().toEpochDay() - time1.toLocalDate().toEpochDay();
        } catch (SQLException e) {
            String errMsg = e.getMessage();
            if (errMsg.contains("FORMAT")) {
                throw new FailParseTime(errMsg.split("FORMAT")[0], errMsg.split("FORMAT")[1]);
            } else {
                throw new FailParseTime(errMsg);
            }
        }
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {

        public Function<RtExpr[], RtOp> supplier() {
            return DingoDateDiffOp::new;
        }

        @Override
        public List<String> name() {
            return Arrays.asList("datediff");
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(DingoDateDiffOp.class.getMethod("dateDiff", String.class, String.class));
                return methods;
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e.toString(), e);
                throw new RuntimeException(e);
            }
        }
    }
}
