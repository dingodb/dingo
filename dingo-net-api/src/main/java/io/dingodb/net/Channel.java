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

import io.dingodb.common.Location;

import java.util.Map;
import java.util.function.Consumer;

public interface Channel extends AutoCloseable {

    /**
     * Return channel id.
     */
    long channelId();

    /**
     * Send message to remote-end. If it is a new channel, then the message must specify the tag, the remote-end will
     * use the tag to determine who will process the message from this channel
     */
    void send(Message msg);

    void send(Message message, boolean sync);

    void send(BufferOutputStream content, boolean sync);

    /**
     * Set message listener on the channel. When the remote-end returns a message, listener will be notified.
     */
    void setMessageListener(MessageListener listener);

    /**
     * Set close listener on the channel. When the channel close, listener will be notified.
     */
    void setCloseListener(Consumer<Channel> listener);

    /**
     * Returns channel auth content.
     * Result is auth tag to auth return {certificate, return} mapping.
     *
     * @return auth map
     */
    Map<String, Object[]> auth();

    /**
     * Returns current channel status {@link Status}.
     */
    Status status();

    /**
     * Returns remote-end location.
     */
    Location remoteLocation();

    Channel cloneChannel();

    BufferOutputStream getOutputStream(int size);

    /**
     * Returns true if current channel available.
     */
    default boolean isActive() {
        return status() == Status.ACTIVE;
    }

    /**
     * Returns true if current channel is closed.
     */
    default boolean isClosed() {
        return status() == Status.CLOSE;
    }

    @Override
    void close();

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
         * Wait.
         */
        WAIT,
        /**
         * Close.
         */
        CLOSE
    }
}
