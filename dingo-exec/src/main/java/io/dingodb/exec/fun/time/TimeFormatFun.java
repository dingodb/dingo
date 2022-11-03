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
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.op.RtStringFun;
import io.dingodb.expr.runtime.utils.DateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Time;

@Slf4j
public class TimeFormatFun extends RtStringFun {
    public static final String NAME = "time_format";
    private static final long serialVersionUID = 6543488500036739938L;

    public TimeFormatFun(RtExpr[] paras) {
        super(paras);
    }

    @Override
    protected Object fun(Object @NonNull [] values) {
        Time value = (Time) values[0];
        if (values.length < 2) {
            return DateTimeUtils.timeFormat(value);
        }
        String format = (String) values[1];
        return DingoDateTimeUtils.timeFormat(value, format);
    }
}
