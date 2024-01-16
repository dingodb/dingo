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

package io.dingodb.meta.entity;

public enum IndexType {
    SCALAR(false),
    VECTOR_FLAT(true),
    VECTOR_IVF_FLAT(true),
    VECTOR_IVF_PQ(true),
    VECTOR_HNSW(true),
    VECTOR_DISKANN(true),
    VECTOR_BRUTEFORCE(true);

    public final boolean isVector;

    IndexType(boolean isVector) {
        this.isVector = isVector;
    }
}
