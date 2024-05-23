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

package io.dingodb.calcite.utils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public final class RelNodeCache {
    private static final LoadingCache<String, Map<String, RelNode>> relNodeCache;

    static {
        relNodeCache = CacheBuilder.newBuilder()
            .maximumSize(4096)
            .expireAfterAccess(60, TimeUnit.MINUTES)
            .expireAfterWrite(60, TimeUnit.MINUTES)
            .build(new CacheLoader<String, Map<String, RelNode>>() {
                @Override
                public @NonNull Map<String, RelNode> load(@NonNull String schema) {
                    return new ConcurrentHashMap<>();
                }
            });
    }

    private RelNodeCache() {
    }

    public static RelNode getRelNode(String schema, String r, boolean prepare) {
        if (!prepare) {
            return null;
        }
        Map<String, RelNode> relNodeMap;
        try {
            relNodeMap = relNodeCache.get(schema);
            return relNodeMap.get(r);
        } catch (ExecutionException e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    public static void setRelNode(String schema, String sql, RelNode relNode, boolean prepare) {
        if (!prepare) {
            return;
        }
        Map<String, RelNode> relNodeMap;
        try {
            relNodeMap = relNodeCache.get(schema);
            relNodeMap.put(sql, relNode);
        } catch (ExecutionException e) {
            log.error(e.getMessage(), e);
        }
    }

    public static void clearRelNode(String tableName) {
        Collection<Map<String, RelNode>> collection = relNodeCache.asMap().values();
        collection.forEach(map -> {
            map.keySet().removeIf(key -> key.contains(tableName)
              || key.contains(tableName.toUpperCase()) || key.contains(tableName.toLowerCase()));
        });
    }
}
