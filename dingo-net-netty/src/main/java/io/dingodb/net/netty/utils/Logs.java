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

package io.dingodb.net.netty.utils;

import io.dingodb.net.netty.connection.Connection;
import io.dingodb.net.netty.packet.Packet;
import io.dingodb.net.netty.utils.StackTraces;
import org.slf4j.Logger;

import static io.dingodb.net.netty.packet.PacketType.isPingPong;

public final class Logs {

    private Logs() {
    }

    public static void packetDbg(Logger log, Connection connection, Packet packet) {
        if ((log.isDebugEnabled() && !isPingPong(packet.header().type())) || log.isTraceEnabled()) {
            log.debug(
                "{}::Packet [{}/{}] <------ [{}/{}], mode: [{}], type: [{}], msg no: [{}].",
                StackTraces.traceLog(),
                connection.localAddress(),
                packet.header().channelId(),
                connection.remoteAddress(),
                packet.header().targetChannelId(),
                packet.header().mode(),
                packet.header().type(),
                packet.header().msgNo()
            );
        }
    }

    public static void packetErr(Logger log, Connection connection, Packet packet, String errMsg, Exception...ex) {
        if (isPingPong(packet.header().type()) && !log.isTraceEnabled()) {
            return;
        }
        log.error(
            "{}::Packet [{}/{}] <------ [{}/{}], mode: [{}], type: [{}], msg no: [{}], {}.",
            StackTraces.traceLog(),
            connection.localAddress(),
            packet.header().channelId(),
            connection.remoteAddress(),
            packet.header().targetChannelId(),
            packet.header().mode(),
            packet.header().type(),
            packet.header().msgNo(),
            errMsg
        );
    }

    public static String providerGetNull(Object obj) {
        return String.format("%s get return null.", obj.getClass());
    }

}
