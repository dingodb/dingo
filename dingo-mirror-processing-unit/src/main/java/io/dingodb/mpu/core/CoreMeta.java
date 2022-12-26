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

package io.dingodb.mpu.core;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class CoreMeta implements Comparable<CoreMeta> {

    public final CommonId id;
    public final CommonId coreId;
    public final Location location;

    public final int priority;

    @EqualsAndHashCode.Include
    public final String label;

    public CoreMeta(CommonId id, Location location) {
        this(id, id, location);
    }

    public CoreMeta(CommonId id, Location location, int priority) {
        this(id, id, location, priority);
    }

    public CoreMeta(CommonId id, CommonId coreId, Location location) {
        this(id, coreId, location, 0);
    }

    public CoreMeta(CommonId id, CommonId coreId, Location location, int priority) {
        this.id = id;
        this.coreId = coreId;
        this.location = location;
        this.priority = priority;
        this.label = String.format("%s/%s", coreId, id);
    }

    @Override
    public int compareTo(CoreMeta other) {
        if (other == null) {
            return 1;
        }
        return priority == other.priority ? id.compareTo(other.id) : Integer.compare(priority, other.priority);
    }

}
