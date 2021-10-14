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

import java.net.InetSocketAddress;

public interface Channel extends AutoCloseable {

    /**
     * Send message to remote-end. If it is a new channel, then the message must specify the tag, the remote-end will
     * use the tag to determine who will process the message from this channel
     */
    void send(Message msg);

    /**
     * Register message listeners on the channel. When the remote-end returns a message, listeners will be notified.
     */
    void registerMessageListener(MessageListener listener);

    /**
     * Returns current channel status {@link Status}.
     */
    Status status();

    /**
     * Returns current local address.
     */
    InetSocketAddress localAddress();

    /**
     * Returns remote-end address.
     */
    InetSocketAddress remoteAddress();

    enum Status {
        /**
         * New channel.
         */
        NEW,
        /**
         * When send out message or register message listener change to this status.
         */
        ACTIVE,
        /**
         * Remote-end unreachable.
         */
        INACTIVE,
        /**
         * Close.
         */
        CLOSE
    }

}
