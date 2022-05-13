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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;

@Slf4j
public class DingoDateDiffOp extends RtFun {

    public DingoDateDiffOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Override
    public int typeCode() {
        return TypeCode.LONG;
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
        Timestamp timestamp0;
        if (!(values[0] instanceof Timestamp)) {
            LocalDateTime ldt0 = LocalDateTime.ofEpochSecond((Long) values[0] / 1000, 0, ZoneOffset.UTC);
            timestamp0 = new Timestamp(ldt0.toEpochSecond(DingoDateTimeUtils.getLocalZoneOffset()) * 1000);
        } else {
            timestamp0 = (Timestamp) values[0];
        }
        Timestamp timestamp1;
        if (!(values[1] instanceof Timestamp)) {
            LocalDateTime ldt1 = LocalDateTime.ofEpochSecond((Long) values[1] / 1000, 0, ZoneOffset.UTC);
            timestamp1 = new Timestamp(ldt1.toEpochSecond(DingoDateTimeUtils.getLocalZoneOffset()) * 1000);
        } else {
            timestamp1 = (Timestamp) values[1];
        }
        return dateDiff(timestamp0, timestamp1);
    }

    public static Long dateDiff(Timestamp timestamp0, Timestamp timestamp1) {
        LocalDate ld0 = timestamp0.toLocalDateTime().atZone(ZoneOffset.UTC).toLocalDate();
        LocalDate ld1 = timestamp1.toLocalDateTime().atZone(ZoneOffset.UTC).toLocalDate();
        return ld0.toEpochDay() - ld1.toEpochDay();
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
                methods.add(DingoDateDiffOp.class.getMethod("dateDiff", Timestamp.class, Timestamp.class));
                return methods;
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e.toString(), e);
                throw new RuntimeException(e);
            }
        }
    }
}
