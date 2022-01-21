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

package io.dingodb.net.netty.handler;

import io.dingodb.common.util.Optional;
import io.dingodb.net.netty.connection.Connection;
import io.dingodb.net.netty.packet.Packet;
import io.dingodb.net.netty.packet.PacketMode;
import io.dingodb.net.netty.packet.PacketType;
import io.dingodb.net.netty.utils.Logs;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MessageDispatcher {

    public static final MessageDispatcher INSTANCE = new MessageDispatcher();
    private final Map<PacketMode, Map<PacketType, MessageHandler>> handlers;

    private MessageDispatcher() {
        handlers = new ConcurrentHashMap<>();
        Arrays.stream(PacketMode.values()).forEach(mode -> handlers.put(mode, new ConcurrentHashMap<>()));
    }

    public static MessageDispatcher instance() {
        return INSTANCE;
    }

    public void registerHandler(PacketMode mode, PacketType type, MessageHandler handler) {
        if (handlers.get(mode).get(type) == null && handler != null) {
            handlers.get(mode).put(type, handler);
        }
        if (handler != handlers.get(mode).get(type)) {
            throw new RuntimeException("Exist handle");
        }
    }

    public void dispatch(Connection connection, Packet packet) {
        Optional.ofNullable(handlers.get(packet.header().mode()))
            .filter(typeHandlers -> !typeHandlers.isEmpty())
            .map(typeHandlers -> typeHandlers.get(packet.header().type()))
            .ifAbsent(() -> Logs.packetWarn(false, log, connection,packet, "not found handler."))
            .ifPresent(handler -> handler.handle(connection, packet));
    }

}
