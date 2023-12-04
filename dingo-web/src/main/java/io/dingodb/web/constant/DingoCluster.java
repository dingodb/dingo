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

package io.dingodb.web.constant;

import java.util.HashMap;
import java.util.Map;

public class DingoCluster {
    public static final Map<Integer, String> regionMap = new HashMap<>();

    static {
        regionMap.put(0, "REGION_NONE");
        regionMap.put(1, "REGION_NEW");
        regionMap.put(2, "REGION_NORMAL");
        regionMap.put(3, "REGION_EXPAND");
        regionMap.put(4, "REGION_EXPANDING");
        regionMap.put(5, "REGION_EXPANDED");
        regionMap.put(6, "REGION_SHRINK");
        regionMap.put(7, "REGION_SHIRINKING");
        regionMap.put(8, "REGION_SHRANK");
        regionMap.put(9, "REGION_DELETE");
        regionMap.put(10, "REGION_DELETING");
        regionMap.put(11, "REGION_DELETED");
        regionMap.put(12, "REGION_SPLIT");
        regionMap.put(13, "REGION_SPLITTING");
        regionMap.put(14, "REGION_SPLITED");
        regionMap.put(15, "REGION_MERGE");
        regionMap.put(16, "REGION_MERGING");
        regionMap.put(17, "REGION_MERGED");
        regionMap.put(20, "REGION_ILLEGAL");
        regionMap.put(21, "REGION_STANDBY");
        regionMap.put(22, "REGION_TOMBSTONE");
    }
}
