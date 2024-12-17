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

package io.dingodb.store.api.transaction.data.checkstatus;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
@Setter
@Builder
@EqualsAndHashCode
public class AsyncResolveData {

    private long commitTs;
    private boolean missingLock;
    private Set<byte[]> keys;

    @Override
    public String toString() {
        return "AsyncResolveData{" +
            "commitTs=" + commitTs +
            ", missingLock=" + missingLock +
            ", keys=" + keys.stream()
            .map(Arrays::toString)
            .collect(Collectors.joining(", ", "{", "}")) +
            '}';
    }
}
