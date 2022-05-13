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
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;

@Slf4j
public class DingoDateFormatOp extends RtFun {

    public DingoDateFormatOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Override
    public int typeCode() {
        return TypeCode.STRING;
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
        String time = (String) values[0];
        if (time.isEmpty()) {
            return null;
        }
        String formatStr = DingoDateTimeUtils.defaultDateFormat();
        if (values.length == 2) {
            formatStr = (String)values[1];
        }
        return dateFormat(time, formatStr);
    }

    public static String dateFormat(final String time, final String formatStr) {
        try {
            LocalDateTime dateTime = DingoDateTimeUtils.convertToDatetime(time);
            if (formatStr.equals(DingoDateTimeUtils.defaultTimeFormat())) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(formatStr);
                return dateTime.format(formatter);
            } else {
                return DingoDateTimeUtils.processFormatStr(dateTime, formatStr);
            }
        } catch (SQLException e) {
            throw new FailParseTime(e.getMessage().split("FORMAT")[0], e.getMessage().split("FORMAT")[1]);
        }
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {

        public Function<RtExpr[], RtOp> supplier() {
            return DingoDateFormatOp::new;
        }

        @Override
        public List<String> name() {
            return Arrays.asList("date_format");
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(DingoDateFormatOp.class.getMethod("dateFormat", String.class, String.class));
                return methods;
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e.toString(), e);
                throw new RuntimeException(e);
            }
        }
    }
}
