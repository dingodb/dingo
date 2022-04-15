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

package io.dingodb.raft.kv.storage;

import java.util.Iterator;

public interface SeekableIterator<P, T> extends Iterator<T> {

    /**
     * Returns the next element index in the iteration.
     */
    T current();

    /**
     * Set the position for the {@link #next()}.
     * @param position position
     */
    void seek(final P position);

    /**
     * Set the position of {@link #next()} to the first position.
     */
    void seekToFirst();

    /**
     * Set the position of {@link #next()} to the last position.
     */
    void seekToLast();

}
