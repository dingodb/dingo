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

package io.dingodb.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.net.InetSocketAddress;

@ToString(of = {"host", "port"})
public class Location implements Serializable {
    private static final long serialVersionUID = 4013504472715015258L;

    @JsonProperty("host")
    @Getter
    private final String host;
    @JsonProperty("port")
    @Getter
    private final int port;

    private transient String url;

    @JsonCreator
    public Location(
        @JsonProperty("host") String host,
        @JsonProperty("port") int port
    ) {
        this.host = host;
        this.port = port;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public String url() {
        if (this.url == null) {
            this.url = host + ":" + port;
        }
        return this.url;
    }

    public static Location parseUrl(String url) {
        if (url != null) {
            String[] location = url.split(":");
            if (location.length < 2) {
                return null;
            }
            return new Location(location[0], Integer.parseInt(location[1]));
        }
        return null;
    }

    public InetSocketAddress toSocketAddress() {
        return new InetSocketAddress(host, port);
    }

    public boolean equals(final Object other) {
        return other == this || other instanceof Location && url().equals(((Location) other).url());
    }

    public int hashCode() {
        return url().hashCode();
    }
}
