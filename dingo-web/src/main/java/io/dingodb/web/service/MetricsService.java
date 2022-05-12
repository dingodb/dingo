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

package io.dingodb.web.service;

import io.dingodb.common.Location;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.Part;
import io.dingodb.server.api.MetaServiceApi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class MetricsService {

    @Autowired
    private MetaServiceApi metaServiceApi;

    public List<Location> partTable(String schema, String name) {
        NavigableMap<ByteArrayUtils.ComparableByteArray, Part> parts = getParts(schema, name);
        LinkedList<Location> locations = new LinkedList<>();
        if (parts != null) {
            parts.values().forEach(part -> locations.addAll(part.getReplicates()));
        }
        return locations.stream().distinct().collect(Collectors.toList());
    }

    public Part partReplica(String schema, String name) {
        NavigableMap<ByteArrayUtils.ComparableByteArray, Part> parts = getParts(schema, name);
        if (parts != null) {
            return parts.values().stream().findAny().get();
        }
        return null;
    }

    private NavigableMap<ByteArrayUtils.ComparableByteArray, Part> getParts(String schema, String name) {
        if (schema != null && !schema.isEmpty()) {
            // todo
            return null;
        } else {
            return metaServiceApi.getParts(name.toUpperCase());
        }
    }
}
