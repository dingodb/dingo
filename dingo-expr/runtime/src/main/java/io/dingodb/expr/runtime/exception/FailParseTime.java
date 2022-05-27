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

package io.dingodb.expr.runtime.exception;

import lombok.Getter;

public class FailParseTime extends RuntimeException {
    private static final long serialVersionUID = 2421873939295156623L;

    @Getter
    private final String time;
    @Getter
    private final String format;

    /**
     * Thrown if parsing a string to time failed.
     *
     * @param time   the time string
     * @param format the specified time
     */
    public FailParseTime(String time, String format) {
        super(
            "Error in parsing string \"" + time + "\" to time, format is \"" + format + "\"."
        );
        this.time = time;
        this.format = format;
    }

    public FailParseTime(String time) {
        super(
            "Error in parsing \"" + time + "\" to time/date/datetime"
        );
        this.time = time;
        this.format = "";
    }
}
