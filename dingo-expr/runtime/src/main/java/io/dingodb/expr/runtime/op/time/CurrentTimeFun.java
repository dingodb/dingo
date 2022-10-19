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
import io.dingodb.expr.runtime.EvalEnv;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.expr.runtime.exception.NeverRunToHere;
import io.dingodb.expr.runtime.op.RtEnvFun;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import io.dingodb.func.DingoFuncProvider;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Method;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class CurrentTimeFun extends RtEnvFun {
    private static final long serialVersionUID = -2956287953719703834L;

    public CurrentTimeFun(RtExpr[] paras) {
        super(paras);
    }

    public static Time getCurrentTime() {
        throw new NeverRunToHere("should never be called.");
    }

    @Override
    protected Object envFun(Object[] values, @Nullable EvalEnv env) {
        return env != null ? DateTimeUtils.currentTime(env.getTimeZone()) : DateTimeUtils.currentTime();
    }

    @Override
    public int typeCode() {
        return TypeCode.TIME;
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {
        @Override
        public List<String> name() {
            return Arrays.asList("current_time", "curtime");
        }

        @Override
        public List<Method> methods() {
            List<Method> methods = new ArrayList<>();
            try {
                methods.add(CurrentTimeFun.class.getMethod("getCurrentTime"));
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e, e);
                throw new RuntimeException(e);
            }
            return methods;
        }
    }
}
