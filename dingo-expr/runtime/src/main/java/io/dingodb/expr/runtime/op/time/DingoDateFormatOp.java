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
import io.dingodb.expr.runtime.op.time.utils.DingoDateTimeUtils;
import io.dingodb.func.DingoFuncProvider;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
        Timestamp originDateTime;
        if (values[0] instanceof Timestamp) {
            originDateTime = (Timestamp) values[0];
        } else {
            LocalDateTime ldt = LocalDateTime.ofEpochSecond(((Long) values[0]) / 1000,
                0 , ZoneOffset.UTC);
            originDateTime = Timestamp.valueOf(ldt);

        }
        String formatStr = DingoDateTimeUtils.defaultDateFormat();
        if (values.length == 2) {
            formatStr = (String)values[1];
        }
        return dateFormat(originDateTime, formatStr);
    }

    // Todo this place only checks the type thing. so we just return ""
    public static String dateFormat(final Timestamp dateTime, final String formatStr) {
        if (formatStr.equals(DingoDateTimeUtils.defaultTimeFormat())) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(formatStr);
            return dateTime.toLocalDateTime().format(formatter);
        } else {
            return DingoDateTimeUtils.processFormatStr(dateTime.toLocalDateTime(), formatStr);
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
                methods.add(DingoDateFormatOp.class.getMethod("dateFormat", Timestamp.class, String.class));
                return methods;
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e.toString(), e);
                throw new RuntimeException(e);
            }
        }
    }
}
