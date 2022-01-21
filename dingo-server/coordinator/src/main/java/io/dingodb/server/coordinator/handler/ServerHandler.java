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

package io.dingodb.server.coordinator.handler;

import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.net.Channel;
import io.dingodb.net.Message;
import io.dingodb.server.protocol.code.BaseCode;
import io.dingodb.server.protocol.code.Code;

import java.nio.ByteBuffer;

import static io.dingodb.server.protocol.ServerError.UNSUPPORTED_CODE;
import static io.dingodb.server.protocol.code.BaseCode.PONG;

public interface ServerHandler {

    default void onMessage(Message message, Channel channel) {
        ByteBuffer buffer = ByteBuffer.wrap(message.toBytes());
        Code code = Code.valueOf(PrimitiveCodec.readZigZagInt(buffer));
        if (code instanceof BaseCode) {
            switch ((BaseCode) code) {
                case PING:
                    channel.registerMessageListener(this::onMessage);
                    channel.send(PONG.message());
                    break;
                case OTHER:
                    break;
                default:
                    channel.send(UNSUPPORTED_CODE.message());
                    break;
            }
        }
    }

}
