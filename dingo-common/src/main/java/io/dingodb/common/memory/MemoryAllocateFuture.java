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

import com.google.common.util.concurrent.ListenableFuture;

public class MemoryAllocateFuture {

    private ListenableFuture<?> allocateFuture = null;

    public MemoryAllocateFuture() {

    }

    public void reset() {
        allocateFuture = null;
    }

    public void setAllocateFuture(ListenableFuture<?> allocateFuture) {
        this.allocateFuture = allocateFuture;
    }

    public ListenableFuture<?> getAllocateFuture() {
        return allocateFuture;
    }

}
