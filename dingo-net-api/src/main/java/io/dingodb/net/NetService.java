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

public interface NetService extends AutoCloseable {

    /**
     * Returns this service location.
     *
     * @return this service location
     */
    //Location currentLocation();

    /**
     * Return a new channel connected to the remote-end.
     *
     * @param netAddress remote node
     * @return the channel connected to the remote node
     */
    Channel newChannel(NetAddress netAddress) throws Exception;

    /**
     * Register {@link MessageListenerProvider} on the net service,  When the remote-end send a message to current
     * service, will create new {@link MessageListener} instance to listen new channel.
     */
    void registerMessageListenerProvider(Tag tag, MessageListenerProvider listenerProvider);

    /**
     * Unregister {@link MessageListenerProvider} on the net service.
     */
    void unregisterMessageListenerProvider(Tag tag, MessageListenerProvider listenerProvider);

    /**
     * Listen the port, When receive message, notify the registered listener.
     *
     * @param port listen port
     */
    void listenPort(int port) throws Exception;
}
