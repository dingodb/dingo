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

package io.dingodb.net.netty.handler.impl;

import com.google.auto.service.AutoService;
import io.dingodb.common.concurrent.ThreadPoolBuilder;
import io.dingodb.net.Message;
import io.dingodb.net.netty.connection.Connection;
import io.dingodb.net.netty.handler.MessageDispatcher;
import io.dingodb.net.netty.handler.MessageHandler;
import io.dingodb.net.netty.packet.Packet;
import io.dingodb.net.netty.packet.PacketMode;
import io.dingodb.net.netty.packet.PacketType;
import io.dingodb.net.netty.utils.Logs;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;

@Slf4j
public class UserDefineMessageHandler implements MessageHandler {

    public static final UserDefineMessageHandler INSTANCE = new UserDefineMessageHandler();

    private UserDefineMessageHandler() {
        MessageDispatcher.instance().registerHandler(PacketMode.USER_DEFINE, PacketType.USER_DEFINE, this);
        MessageDispatcher.instance().registerHandler(PacketMode.API, PacketType.INVOKE, this);
        MessageDispatcher.instance().registerHandler(PacketMode.API, PacketType.RETURN, this);
    }

    public static UserDefineMessageHandler instance() {
        return INSTANCE;
    }

    @Override
    public void handle(Connection<Message> connection, Packet<Message> packet) {
        Logs.packetDbg(false, log, connection, packet);
        connection.receive(packet);
    }

    @AutoService(MessageHandler.Provider.class)
    public static class Provider implements MessageHandler.Provider {
        private final UserDefineMessageHandler instance = instance();

        @Override
        public MessageHandler get() {
            return instance;
        }
    }
}
