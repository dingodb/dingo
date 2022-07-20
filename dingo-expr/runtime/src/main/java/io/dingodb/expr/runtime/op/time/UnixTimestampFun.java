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
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import io.dingodb.func.DingoFuncProvider;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

@Slf4j
public class UnixTimestampFun extends RtFun {

    public UnixTimestampFun(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    public static long unixTimestamp() {
        return unixTimestamp(DateTimeUtils.currentTimestamp());
    }

    public static long unixTimestamp(@Nonnull Timestamp value) {
        return Math.floorDiv(value.getTime(), 1000L);
    }

    @Override
    public int typeCode() {
        return TypeCode.LONG;
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
        return values.length > 0 ? unixTimestamp((Timestamp) values[0]) : unixTimestamp();
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {
        @Override
        public List<String> name() {
            return Collections.singletonList("unix_timestamp");
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(UnixTimestampFun.class.getMethod("unixTimestamp"));
                methods.add(UnixTimestampFun.class.getMethod("unixTimestamp", Timestamp.class));
                return methods;
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e, e);
                throw new RuntimeException(e);
            }
        }
    }
}
