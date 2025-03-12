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

package io.dingodb.exec.fun;

import io.dingodb.expr.common.type.Type;
import io.dingodb.expr.common.type.Types;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.UnaryOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Serial;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

public class DateFun extends UnaryOp {

    public static final DateFun INSTANCE = new DateFun();

    public static final String NAME = "GETDATE";

    @Serial
    private static final long serialVersionUID = -3232758300335248032L;

    private static final List<DateTimeFormatter> FORMATTERS = Arrays.asList(
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:s"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd"),
        DateTimeFormatter.ofPattern("yyyy-M-dd"),
        DateTimeFormatter.ofPattern("yyyy-MM-d"),
        DateTimeFormatter.ofPattern("yyyy.MM.dd HH:mm"),
        DateTimeFormatter.ofPattern("yyyy.MM.dd HH:mm:ss"),
        DateTimeFormatter.ofPattern("yyyy.MM.dd HH:mm:s"),
        DateTimeFormatter.ofPattern("yyyy.MM.dd"),
        DateTimeFormatter.ofPattern("yyyy.M.dd"),
        DateTimeFormatter.ofPattern("yyyy.MM.d"),
        DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm"),
        DateTimeFormatter.ofPattern("yyyy/MM/dd H:mm"),
        DateTimeFormatter.ofPattern("yyyy/MM/d H:mm"),
        DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"),
        DateTimeFormatter.ofPattern("yyyy/MM/dd"),
        DateTimeFormatter.ofPattern("yyyy/M/dd"),
        DateTimeFormatter.ofPattern("yyyy/MM/d"),
        DateTimeFormatter.ofPattern("yyyyMMddHHmmss"),
        DateTimeFormatter.ofPattern("yyyyMMddHHmm"),
        DateTimeFormatter.ofPattern("yyyyMMdd")
    );

    @Override
    public Object evalValue(Object value, ExprConfig config) {
        if (value == null || (value instanceof String && value.toString().isEmpty())) {
            return null;
        } else {
            for (DateTimeFormatter formatter : FORMATTERS) {
                try {
                    LocalDateTime ldt;
                    try {
                        ldt = LocalDateTime.parse(value.toString(), formatter);
                    } catch (Exception e) {
                        LocalDate localDate = LocalDate.parse(value.toString(), formatter);
                        return Date.valueOf(localDate);
                    }
                    return Date.valueOf(ldt.toLocalDate());
                } catch (Exception e) {
                    // ignore
                }
            }
        }
        return null;
    }

    @Override
    public Type getType() {
        return Types.DATE;
    }

    @Override
    public @NonNull String getName() {
        return NAME;
    }
}
