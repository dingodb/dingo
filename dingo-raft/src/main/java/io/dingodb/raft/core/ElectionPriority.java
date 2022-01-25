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

package io.dingodb.raft.core;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class ElectionPriority {
    /**
     * Priority -1 represents this node disabled the priority election function.
     */
    public static final int Disabled = -1;

    /**
     * Priority 0 is a special value so that a node will never participate in election.
     */
    public static final int NotElected = 0;

    /**
     * Priority 1 is a minimum value for priority election.
     */
    public static final int MinValue = 1;
}
