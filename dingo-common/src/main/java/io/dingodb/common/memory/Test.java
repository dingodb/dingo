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

package io.dingodb.common.memory;

import static io.dingodb.common.memory.MemorySetting.DEFAULT_ALLOCATOR_SIZE;

public class Test {
    public static void main(String[] args) {
        long size = -1240;
        allocated(size);
        allocated(-3330);

        allocated(-137330);
    }

    public static void allocated(long left) {
        long amount = -Math.floorDiv(left, DEFAULT_ALLOCATOR_SIZE) * DEFAULT_ALLOCATOR_SIZE;
        System.out.println(amount);
    }
}
