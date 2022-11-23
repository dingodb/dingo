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

import io.dingodb.common.codec.VarNumberCodec;
import io.dingodb.common.error.DingoError;
import io.dingodb.common.error.DingoException;
import io.dingodb.common.error.FormattingError;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.dingodb.common.codec.VarNumberCodec.encodeZigZagInt;

/**
 * Net error. 31XXX Net service error. 32XXX rpc error. 39XXX Other error.
 */
@SuppressWarnings("checkstyle:LineLength")
public enum NetError implements FormattingError {

    OK(0, "OK, no error."),

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
    /**
     * Open channel busy.
     */
    OPEN_CHANNEL_BUSY(31004, "Open channel busy", "Open channel busy"),

    /*********************************   RPC.   *****************************************************/
    EXEC(32001, "Execute error", "Exec %s error, thread: [%s], message: [%s]."),
    EXEC_INTERRUPT(32002, "Exec interrupted error.", "Exec %s interrupted, thread: [%s], message: [%s]."),
    EXEC_TIMEOUT(32003, "Execute timeout.", "Exec %s timeout, thread: [%s], message: [%s]."),
    API_NOT_FOUND(32004, "Api not found.", "Api %s not found."),

    /*********************************   Unknown.   *****************************************************/

    /**
     * Unknown error.
     */
    UNKNOWN(39000, "Unknown.", "Unknown error, message: [%s]"),
    IO(39001, "IO error, please check log.", "IO error, message: [%s]"),
    ;

    private final int code;
    private final String info;
    private final String format;

    NetError(int code, String info) {
        this.code = code;
        this.info = info;
        this.format = info;
        addCache(code, this);
    }

    NetError(int code, String info, String format) {
        this.code = code;
        this.info = info;
        this.format = format;
        addCache(code, this);
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

    public Message message() {
        return new Message(encodeZigZagInt(getCode()));
    }

    public static Message message(DingoException err) {
        return new Message(encodeZigZagInt(err.getCode()));
    }

    private static Map<Integer, NetError> valueOfCache;

    private static void addCache(int code, NetError error) {
        if (valueOfCache == null) {
            valueOfCache = new HashMap<>();
        }
        valueOfCache.put(code, error);
    }

    public static FormattingError valueOf(Integer code) {
        if (code == OK.code) {
            return null;
        }
        return valueOfCache.computeIfAbsent(
            code,
            k -> Arrays.stream(NetError.values()).filter(c -> c.code == code).findAny().orElse(null)
        );
    }

    public static FormattingError valueOf(byte[] bytes) {
        return valueOf(VarNumberCodec.readZigZagInt(bytes));
    }

    public static FormattingError valueOf(ByteBuffer buffer) {
        return valueOf(VarNumberCodec.readZigZagInt(buffer));
    }
}
