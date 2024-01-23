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

package io.dingodb.net.netty;

import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.net.Message;


public class Constant {
    public static final byte[] EMPTY_BYTES = new byte[0];
    public static final Object[] API_EMPTY_ARGS = new Object[0];

    public static final String API_OK = "API_OK";
    public static final String API_ERROR = "API_ERROR";
    public static final String API_CANCEL = "API_CANCEL";
    public static final String FILE_TRANSFER = "FILE_TRANSFER";
    public static final String LISTENER = "LISTENER";

    public static final byte[] API_OK_B = PrimitiveCodec.encodeString(API_OK);
    public static final byte[] API_ERROR_B = PrimitiveCodec.encodeString(API_ERROR);
    public static final byte[] API_CANCEL_B = PrimitiveCodec.encodeString(API_CANCEL);
    public static final byte[] FILE_TRANSFER_B = PrimitiveCodec.encodeString(FILE_TRANSFER);

    public static final Message API_VOID = new Message(API_OK, EMPTY_BYTES);

    // message type
    public static final byte USER_DEFINE_T = 1;
    public static final byte COMMAND_T = 2;
    public static final byte API_T = 3;
    public static final byte HANDSHAKE_T = 4;

    // command message code
    public static final byte PING_C = 1;
    public static final byte PONG_C = 2;
    public static final byte ACK_C = 3;
    public static final byte HANDSHAKE_C = 4;
    public static final byte CLOSE_C = 5;
    public static final byte ERROR_C = 6;

    public static final String AUTH = "auth";
    public static final String HANDSHAKE = "handshake";

    public static final String CLIENT = "client";
    public static final String SERVER = "server";

    public static final long C2S = 0;
    public static final long S2C = Long.MIN_VALUE;
}
