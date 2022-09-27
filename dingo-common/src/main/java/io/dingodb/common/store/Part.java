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

package io.dingodb.common.store;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class Part {

    public enum PartType {
        ROW_STORE,
        COLUMN_STORE,
    }

    private CommonId id;
    private CommonId instanceId;
    private CommonId replicateId;
    private byte[] start;
    private byte[] end;
    @Builder.Default
    private PartType type = PartType.ROW_STORE;

    private CommonId leader;
    private Location leaderLocation;
    private List<CommonId> replicates;
    private List<Location> replicateLocations;
    private int version;
    private int ttl;


    @Override
    public String toString() {
        return new StringJoiner(", ", Part.class.getSimpleName() + "[", "]").add("id=" + id)
            .add("instanceId=" + instanceId)
            .add("start=" + Arrays.toString(start))
            .add("end=" + Arrays.toString(end))
            .add("type=" + type)
            .add("leader=" + leaderLocation)
            .add("replicates=" + replicateLocations)
            .add("version=" + version)
            .add("ttl=" + ttl)
            .toString();
    }
}
