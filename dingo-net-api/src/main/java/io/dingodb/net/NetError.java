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

package io.dingodb.net;

import io.dingodb.common.error.DingoError;
import io.dingodb.common.error.FormattingError;

/**
 * Net error. 31XXX Net service error. 32XXX Application error. 39XXX Other error.
 */
@SuppressWarnings("checkstyle:LineLength")
public enum NetError implements FormattingError {

    /*********************************   Net service.   *****************************************************/

    /**
     * Handshake failed.
     */
    HANDSHAKE(31001, "Handshake failed.", "Handshake failed. Reason: [%s]"),
    /**
     * Open channel time out.
     */
    OPEN_CHANNEL_TIME_OUT(31002, "Open channel time out.", "Open channel time out."),
    /**
     * Open channel interrupt.
     */
    OPEN_CHANNEL_INTERRUPT(31003, "Open channel interrupt", "Open channel interrupt"),
    /**
     * Open channel time out.
     */
    OPEN_CONNECTION_TIME_OUT(31002, "Open connection time out.", "Open connection time out, remote [%s]"),
    /**
     * Open channel interrupt.
     */
    OPEN_CONNECTION_INTERRUPT(31003, "Open connection interrupt", "Open connection interrupt, remote [%s]"),

    /*********************************   Application.   *****************************************************/

    /*********************************   Unknown.   *****************************************************/

    /**
     * Unknown error.
     */
    UNKNOWN(39000, "Unknown.", "Unknown error, message: [%s]"),
    WRAPPED(39001, "Wrapped application error.", "Message: [%s]");

    private final int code;
    private final String info;
    private final String format;

    NetError(int code, String info, String format) {
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
