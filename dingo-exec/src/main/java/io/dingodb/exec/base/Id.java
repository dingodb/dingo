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

package io.dingodb.exec.base;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.EqualsAndHashCode;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@EqualsAndHashCode(of = {"value"})
public final class Id implements Comparable<Id> {
    public static Id NULL = new Id("");

    @JsonValue
    private final String value;

    @JsonCreator
    public Id(String value) {
        this.value = value;
    }

    public static @NonNull Id random() {
        return new Id(UUID.randomUUID().toString());
    }

    public static @NonNull Id fromBytes(byte[] bytes) {
        return new Id(new String(bytes, StandardCharsets.UTF_8));
    }

    @Override
    public int compareTo(@NonNull Id obj) {
        return value.compareTo(obj.value);
    }

    @Override
    public String toString() {
        return value;
    }

    public byte @NonNull [] toBytes() {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
