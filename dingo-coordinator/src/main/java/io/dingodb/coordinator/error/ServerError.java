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

package io.dingodb.coordinator.error;

import io.dingodb.common.error.DingoError;
import io.dingodb.common.error.FormattingError;

/**
 * Server error. 11XXX Server error. 12XXX Meta service error. 19XXX other error.
 */
@SuppressWarnings("checkstyle:LineLength")
public enum ServerError implements FormattingError {

    /*********************************   Server.   *****************************************************/

    /*********************************   Meta service.   *****************************************************/
    /**
     * 121xx Create table error. 122xx Drop table error. 129xx Other.
     */

    CREATE_META_SUCCESS_WAIT_LEADER_FAILED(12101, "Meta info add success, but wait leader time out.",
        "Table [%s] meta add success, but wait all partition leader success time out."),

    CHECK_AND_WAIT_HELIX_ACTIVE(12901, "Check helix status error.", "Check helix status and wait active."),

    /*********************************   Other.   *****************************************************/

    UNKNOWN(19000, "Unknown.", "Unknown error, message: [%s]"),

    TO_JSON_ERROR(19301, "To json error.", "To json error, happened in [%s], error message: -[%s]-");

    private final int code;
    private final String info;
    private final String format;

    ServerError(int code, String info, String format) {
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
