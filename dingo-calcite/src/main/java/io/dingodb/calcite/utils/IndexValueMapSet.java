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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class IndexValueMapSet<K, V> {
    @Getter
    private final @Nullable Set<Map<K, V>> set;

    public static <K, V> @NonNull IndexValueMapSet<K, V> of(Set<Map<K, V>> set) {
        return new IndexValueMapSet<>(set);
    }

    /**
     * Get an instance meaning anything is not allowed, i.e. `0` in calculating.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @return the instance
     */
    public static <K, V> @NonNull IndexValueMapSet<K, V> zero() {
        return new IndexValueMapSet<>(ImmutableSet.of());
    }

    /**
     * Get an instance meaning everything are allowed, i.e. `1` in calculating.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @return the instance
     */
    public static <K, V> @NonNull IndexValueMapSet<K, V> one() {
        return new IndexValueMapSet<>(null);
    }

    public static <K, V> @NonNull IndexValueMapSet<K, V> single(K index, V value) {
        return new IndexValueMapSet<>(ImmutableSet.of(ImmutableMap.of(index, value)));
    }

    private static <K, V> @Nullable Map<K, V> merge(final @NonNull Map<K, V> s0, final @NonNull Map<K, V> s1) {
        Map<K, V> result = new HashMap<>(s0);
        for (Map.Entry<K, V> entry : s1.entrySet()) {
            K index1 = entry.getKey();
            V value0 = result.get(index1);
            V value1 = entry.getValue();
            if (value0 == null) {
                result.put(index1, value1);
            } else if (value1 != value0) {
                // Conflict
                return null;
            }
        }
        return result;
    }

    boolean isOne() {
        return set == null;
    }

    public @NonNull IndexValueMapSet<K, V> or(@NonNull IndexValueMapSet<K, V> obj) {
        if (set == null || obj.set == null) {
            return one();
        }
        return new IndexValueMapSet<>(
            Stream.concat(set.stream(), obj.set.stream())
                .collect(Collectors.toSet())
        );
    }

    public @NonNull IndexValueMapSet<K, V> and(@NonNull IndexValueMapSet<K, V> obj) {
        if (set == null) {
            return obj;
        }
        if (obj.set == null) {
            return this;
        }
        Set<Map<K, V>> newSet = new HashSet<>();
        for (Map<K, V> item0 : set) {
            for (Map<K, V> item1 : obj.set) {
                Map<K, V> newItem = merge(item0, item1);
                if (newItem != null) {
                    newSet.add(newItem);
                }
            }
        }
        return new IndexValueMapSet<>(newSet);
    }
}
