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

package io.dingodb.exec.fun.time;

import io.dingodb.exec.utils.DingoDateTimeUtils;
import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.runtime.EvalEnv;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.op.RtEnvFun;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@Slf4j
public class CurrentDateFun extends RtEnvFun {
    public static final String NAME = "current_date";
    private static final long serialVersionUID = -6855307131187820249L;

    public CurrentDateFun(RtExpr[] paras) {
        super(paras);
    }

    @Override
    protected Object envFun(Object @NonNull [] values, @Nullable EvalEnv env) {
        return env != null ? DingoDateTimeUtils.currentDate(env.getTimeZone()) : DingoDateTimeUtils.currentDate();
    }

    @Override
    public int typeCode() {
        return TypeCode.DATE;
    }
}
