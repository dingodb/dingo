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

package io.dingodb.calcite.func;

import com.google.common.collect.ImmutableMap;

import java.sql.Date;
import java.sql.Time;

public final class DingoFunc {
    private DingoFunc() {
    }

    public static final ImmutableMap.Builder<String, String> DINGO_FUNC_LIST = new ImmutableMap.Builder<>();

    static {
        DINGO_FUNC_LIST.put("curdate".toUpperCase(), "curDate");
        DINGO_FUNC_LIST.put("current_date".toUpperCase(), "curDate");

        DINGO_FUNC_LIST.put("curtime".toUpperCase(), "curTime");
        DINGO_FUNC_LIST.put("current_time".toUpperCase(), "curTime");

        DINGO_FUNC_LIST.put("current_timestamp".toUpperCase(), "curTimestamp");
        DINGO_FUNC_LIST.put("now".toUpperCase(), "now");
    }

    public static String now() {
        return  "";
    }

    public static Date curDate() {
        return new Date(System.currentTimeMillis());
    }

    public static Time curTime() {
        return new Time(System.currentTimeMillis());
    }

    public static Long curTimestamp() {
        return 0L;
    }
}
