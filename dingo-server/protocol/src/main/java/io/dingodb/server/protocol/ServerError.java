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

package io.dingodb.server.protocol;

import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.error.DingoError;
import io.dingodb.common.error.DingoException;
import io.dingodb.common.error.FormattingError;
import io.dingodb.net.Message;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.dingodb.common.codec.PrimitiveCodec.encodeZigZagInt;

/**
 * Server error. 11XXX Server error. 12XXX Meta service error. 19XXX other error.
 */
@Slf4j
@SuppressWarnings("checkstyle:LineLength")
public enum ServerError implements FormattingError {

    OK(0, "OK, no error."),

    /*********************************   Server.   *****************************************************/

    UNSUPPORTED_CODE(11000, "Unsupported code.", "Unsupported code [%s]"),

    /*********************************   Meta service.   *****************************************************/
    /**
     * 121xx Create table error. 122xx Drop table error. 129xx Other.
     */
    TABLE_EXIST(12101, "Table exist.", "Create table failed, table exist, name: [%s]"),
    TABLE_NOT_FOUND(12102, "Table not found.", "Table not found, name: [%s]"),
    //CREATE_META_SUCCESS_WAIT_LEADER_FAILED(12101, "Meta info add success, but wait leader time out.",
    //    "Table [%s] meta add success, but wait all partition leader success time out."),

    CHECK_AND_WAIT_HELIX_ACTIVE(12901, "Check helix status error.", "Check helix status and wait active."),

    /*********************************   Other.   *****************************************************/

    UNKNOWN(19000, "Unknown, please check server log.", "Unknown error, message: [%s]"),
    IO(19001, "IO error, please check log.", "IO error, message: [%s]"),

    TO_JSON_ERROR(19301, "To json error.", "To json error, happened in [%s], error message: -[%s]-");

    private final int code;
    private final String info;
    private final String format;

    ServerError(int code, String info) {
        this.code = code;
        this.info = info;
        this.format = info;
    }

    ServerError(int code, String info, String format) {
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
        return Message.builder().content(encodeZigZagInt(getCode())).build();
    }

    public static Message message(DingoException err) {
        return Message.builder().content(encodeZigZagInt(err.getCode())).build();
    }

    private static Map<Integer, ServerError> valueOfCache;

    private static void addCache(int code, ServerError error) {
        if (valueOfCache == null) {
            valueOfCache = new HashMap<>();
        }
        valueOfCache.put(code, error);
    }

    public static ServerError valueOf(Integer code) {
        return valueOfCache.computeIfAbsent(
            code,
            k -> Arrays.stream(ServerError.values()).filter(c -> c.code == code).findAny().orElse(null)
        );
    }

    public static ServerError valueOf(byte[] bytes) {
        Integer code = PrimitiveCodec.readZigZagInt(bytes);
        return valueOfCache.computeIfAbsent(
            code,
            k -> Arrays.stream(ServerError.values()).filter(c -> c.code == code).findAny().orElse(null)
        );
    }

    public static ServerError valueOf(ByteBuffer buffer) {
        Integer code = PrimitiveCodec.readZigZagInt(buffer);
        return valueOfCache.computeIfAbsent(
            code,
            k -> Arrays.stream(ServerError.values()).filter(c -> c.code == code).findAny().orElse(null)
        );
    }
}
