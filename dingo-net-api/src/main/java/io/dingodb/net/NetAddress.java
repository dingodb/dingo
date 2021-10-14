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

import lombok.Builder;

import java.net.InetSocketAddress;

public class NetAddress {

    private final InetSocketAddress socketAddress;

    public NetAddress(InetSocketAddress socketAddress) {
        this.socketAddress = socketAddress;
    }

    @Builder
    public NetAddress(String host, int port) {
        this.socketAddress = new InetSocketAddress(host, port);
    }

    public InetSocketAddress address() {
        return socketAddress;
    }

    public String hostAddress() {
        return socketAddress.getAddress().getHostAddress();
    }

    public String hostName() {
        return socketAddress.getHostName();
    }

    public int port() {
        return socketAddress.getPort();
    }

}
