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

package io.dingodb.server.client.connector.impl;

import io.dingodb.common.Location;
import io.dingodb.common.util.Optional;
import io.dingodb.server.client.config.ClientConfiguration;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class CoordinatorConnector extends ServiceConnector {

    private static final CoordinatorConnector DEFAULT = Optional.ofNullable(ClientConfiguration.instance())
        .map(ClientConfiguration::getCoordinatorExchangeSvrList)
        .map(s -> s.split(","))
        .map(Arrays::asList)
        .map(ss -> ss.stream()
            .map(s -> s.split(":"))
            .map(__ -> new Location(__[0], Integer.parseInt(__[1])))
            .collect(Collectors.toList()))
        .map(CoordinatorConnector::new)
        .orNull();

    public static CoordinatorConnector getDefault() {
        return DEFAULT;
    }

    public CoordinatorConnector(List<Location> addresses) {
        super(null, new HashSet<>(addresses));
    }

}
