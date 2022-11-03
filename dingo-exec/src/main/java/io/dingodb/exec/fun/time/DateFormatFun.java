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

import java.sql.Date;

@Slf4j
public class DateFormatFun extends RtStringFun {
    public static final String NAME = "date_format";
    private static final long serialVersionUID = -8131868303444015382L;

    public DateFormatFun(RtExpr[] paras) {
        super(paras);
    }

    @Override
    protected Object fun(Object @NonNull [] values) {
        Date value = (Date) values[0];
        if (values.length < 2) {
            return DateTimeUtils.dateFormat(value);
        }
        String format = (String) values[1];
        return DingoDateTimeUtils.dateFormat(value, format);
    }
}
