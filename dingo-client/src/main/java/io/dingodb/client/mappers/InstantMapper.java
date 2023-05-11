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

package io.dingodb.client.mappers;


import java.time.Instant;

public class InstantMapper extends TypeMapper {

    @Override
    public Object toDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        Instant instant = (Instant) value;
        return instant.getEpochSecond() * 1_000_000_000 + instant.getNano();
    }

    @Override
    public Object fromDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        long longValue = (Long) value;
        return Instant.ofEpochSecond(longValue / 1_000_000_000, longValue % 1_000_000_000);
    }
}
