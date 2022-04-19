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

import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.expr.runtime.op.RtFun;
import io.dingodb.expr.runtime.op.time.timeformatmap.DateFormatUtil;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import javax.annotation.Nonnull;

public class DingoDateFromUnixTimeOp extends RtFun {
    public DingoDateFromUnixTimeOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Override
    public int typeCode() {
        return TypeCode.TIMESTAMP;
    }

    @Override
    protected Object fun(@Nonnull Object[] values) {
        Long timestamp = 0L;

        String timeStr = String.valueOf(values[0]);
        if (timeStr.length() < 13) {
            timestamp = Long.valueOf(timeStr) * 1000;
        } else if (timeStr.length() == 13) {
            timestamp = Long.valueOf(timeStr);
        }

        if (timestamp < 0) {
            throw new IllegalArgumentException("incorrect value of unix_timestamp for from_unixtime function");
        }

        DateTimeFormatter formatter = DateFormatUtil.getDatetimeFormatter();
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());

        if (values.length == 2) {
            String formatStr = String.valueOf(values[1]);
            formatter = DateTimeFormatter.ofPattern(formatStr);
        }

        return formatter.format(dateTime);
    }
}
