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

package io.dingodb.client.mappers;


import io.dingodb.expr.runtime.utils.DateTimeUtils;

import java.sql.Date;
import java.text.SimpleDateFormat;

public class DateMapper extends TypeMapper {

    @Override
    public Object toDingoFormat(Object value) {
        if (value == null) {
            return null;
        }

        String dateInStr = "";
        if (value instanceof java.util.Date) {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            dateInStr = formatter.format((java.util.Date) value);
        } else if (value instanceof java.sql.Date) {
            dateInStr = value.toString();
        } else {
            throw new IllegalArgumentException("Unsupported date type: " + value.getClass().getName());
        }
        return DateTimeUtils.parseDate(dateInStr);
    }

    @Override
    public Object fromDingoFormat(Object value) {
        if (value == null) {
            return null;
        }

        Date expectedDate = null;
        if (value instanceof java.util.Date) {
            expectedDate = new Date(((java.util.Date) value).getTime());
        } else if (value instanceof java.sql.Date) {
            expectedDate = (Date) value;
        } else {
            throw new IllegalArgumentException("Unsupported date type: " + value.getClass().getName());
        }

        String dateInStr = DateTimeUtils.dateFormat(expectedDate);
        return Date.valueOf(dateInStr);
    }
}
