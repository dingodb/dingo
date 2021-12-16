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

package io.dingodb.dingokv.storage;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public interface KVIterator extends AutoCloseable {
    /**
     * An iterator is either positioned at an entry, or not valid.
     * This method returns true if the iterator is valid.
     *
     * @return true if iterator is valid.
     */
    boolean isValid();

    /**
     * Position at the first entry in the source.  The iterator is Valid()
     * after this call if the source is not empty.
     */
    void seekToFirst();

    /**
     * Position at the last entry in the source.  The iterator is
     * valid after this call if the source is not empty.
     */
    void seekToLast();

    /**
     * Position at the first entry in the source whose key is that or
     * past target.
     *
     * The iterator is valid after this call if the source contains
     * a key that comes at or past target.
     *
     * @param target byte array describing a key or a
     *               key prefix to seek for.
     */
    void seek(final byte[] target);

    /**
     * Position at the first entry in the source whose key is that or
     * before target.
     *
     * The iterator is valid after this call if the source contains
     * a key that comes at or before target.
     *
     * @param target byte array describing a key or a
     *               key prefix to seek for.
     */
    void seekForPrev(final byte[] target);

    /**
     * Moves to the next entry in the source.  After this call, Valid() is
     * true if the iterator was not positioned at the last entry in the source.
     *
     * REQUIRES: {@link #isValid()}
     */
    void next();

    /**
     * Moves to the previous entry in the source.  After this call, Valid() is
     * true if the iterator was not positioned at the first entry in source.
     *
     * REQUIRES: {@link #isValid()}
     */
    void prev();

    /**
     * Return the key for the current entry.  The underlying storage for
     * the returned slice is valid only until the next modification of
     * the iterator.
     *
     * REQUIRES: {@link #isValid()}
     *
     * @return key for the current entry.
     */
    byte[] key();

    /**
     * Return the value for the current entry.  The underlying storage for
     * the returned slice is valid only until the next modification of
     * the iterator.
     *
     * REQUIRES: !AtEnd() &amp;&amp; !AtStart()
     *
     * @return value for the current entry.
     */
    byte[] value();
}
