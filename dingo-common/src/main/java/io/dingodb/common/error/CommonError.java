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

package io.dingodb.common.error;

/**
 * Common error, {@code [1000, 10000)}.
 *
 */
public enum CommonError implements FormattingError {
    EXEC(1001, "Execute error", "Exec %s error, thread: [%s], message: %s."),
    EXEC_INTERRUPT(1002, "Exec interrupted error.", "Exec %s interrupted, thread: [%s], message: [%s]."),
    EXEC_TIMEOUT(1003, "Execute timeout.", "Exec %s timeout, thread: [%s], message: [%s]."),
    ;

    private final int code;
    private final String info;
    private final String format;

    CommonError(int code, String info) {
        this.code = code;
        this.info = info;
        this.format = info;
    }

    CommonError(int code, String info, String format) {
        this.code = code;
        this.info = info;
        this.format = format;
    }

    @Override
    public int getCode() {
        return code;
    }

    @Override
    public String getInfo() {
        return info;
    }

    @Override
    public String getFormat() {
        return format;
    }

    @Override
    public String toString() {
        return DingoError.toString(this);
    }
}

