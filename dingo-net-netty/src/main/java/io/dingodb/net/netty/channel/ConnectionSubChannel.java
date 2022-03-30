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

package io.dingodb.net.netty.channel;

import io.dingodb.net.netty.connection.Connection;
import io.dingodb.net.netty.packet.Packet;

public interface ConnectionSubChannel<M> extends Channel {

    /**
     * Returns the connection where the current sub channel resides.
     */
    Connection<M> connection();

    /**
     * Send handshakePacket to remote-end.
     */
    void send(Packet<M> packet);

    /**
     * Receive package from remote-end.
     *
     * @param packet packet
     */
    void receive(Packet<M> packet);

    /**
     * Get next message seq.
     *
     * @return message seq.
     */
    long nextSeq();

    void start();

    void setChannelPool(Connection.ChannelPool pool);

}
