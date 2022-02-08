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

package io.dingodb.common.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.IOException;
import java.util.Base64;
import javax.annotation.Nonnull;

@EqualsAndHashCode
public class TableId {
    @Getter
    private final byte[] value;

    public TableId(byte[] value) {
        this.value = value;
    }

    @JsonCreator
    @Nonnull
    public static TableId fromBase64(String encoded) throws IOException {
        return new TableId(Base64.getDecoder().decode(encoded));
    }

    @JsonValue
    public String toBase64() {
        return Base64.getEncoder().encodeToString(value);
    }

    @Override
    public String toString() {
        return toBase64();
    }
}
