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

package io.dingodb.net.netty.api;

import io.dingodb.common.codec.ProtostuffCodec;
import io.dingodb.net.Message;
import io.dingodb.net.api.annotation.ApiDeclaration;
import io.dingodb.net.error.ApiTerminateException;
import io.dingodb.net.netty.Versions;
import io.dingodb.net.netty.channel.Channel;
import io.dingodb.net.netty.packet.Command;
import lombok.AllArgsConstructor;

import static io.dingodb.net.Message.API_ERROR;
import static io.dingodb.net.netty.Versions.currentVersion;

public interface HandshakeApi {

    HandshakeApi INSTANCE = new HandshakeApi() {};
    RuntimeException VERSION_EXCEPTION = new RuntimeException("Version not support.");

    @AllArgsConstructor
    class Handshake {
        byte[] code;
        byte version;
        public static final Handshake INSTANCE = new Handshake(Versions.MAGIC_CODE, Versions.currentVersion());
    }

    @ApiDeclaration
    default Command handshake(Channel channel, Handshake handshake) throws InterruptedException {
        if (Versions.checkCode(handshake.code, 0) && currentVersion() == handshake.version) {
            return Command.ACK;
        }
        channel.send(new Message(API_ERROR, ProtostuffCodec.write(VERSION_EXCEPTION)), true);
        channel.connection().close();
        throw new ApiTerminateException("Handshake failed, the version [%s] not support.", handshake.version);
    }

}
