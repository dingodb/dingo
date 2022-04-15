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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

@ToString(of = {"host", "port"})
@EqualsAndHashCode
public class Location implements Serializable {
    private static final long serialVersionUID = 4013504472715015258L;

    @JsonProperty("host")
    @Getter
    private final String host;
    @JsonProperty("port")
    @Getter
    private final int port;

    @JsonCreator
    public Location(
        @JsonProperty("host") String host,
        @JsonProperty("port") int port
    ) {
        this.host = host;
        this.port = port;
    }
}
