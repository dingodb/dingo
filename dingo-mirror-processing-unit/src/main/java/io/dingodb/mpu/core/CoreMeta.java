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

public class CoreMeta implements Comparable<CoreMeta> {

    public final CommonId id;
    public final CommonId coreId;
    public final CommonId mpuId;
    public final Location location;
    public final int priority;
    public final String label;

    public CoreMeta(CommonId id, CommonId coreId, CommonId mpuId, Location location, int priority) {
        this.id = id;
        this.coreId = coreId;
        this.mpuId = mpuId;
        this.location = location;
        this.priority = priority;
        this.label = String.format("%s/%s/%s", mpuId, coreId, id);
    }

    @Override
    public int compareTo(CoreMeta other) {
        if (other == null) {
            return 1;
        }
        return priority == other.priority ? id.compareTo(other.id) : Integer.compare(priority, other.priority);
    }

}
