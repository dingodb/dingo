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

package io.dingodb.raft.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class RecyclableByteBufferList extends ArrayList<ByteBuffer> implements Recyclable {
    private static final long serialVersionUID = -8605125654176467947L;

    private static final int DEFAULT_INITIAL_CAPACITY = 8;

    private int capacity = 0;

    /**
     * Create a new empty {@link RecyclableByteBufferList} instance
     */
    public static RecyclableByteBufferList newInstance() {
        return newInstance(DEFAULT_INITIAL_CAPACITY);
    }

    /**
     * Create a new empty {@link RecyclableByteBufferList} instance with the given capacity.
     */
    public static RecyclableByteBufferList newInstance(final int minCapacity) {
        final RecyclableByteBufferList ret = recyclers.get();
        ret.ensureCapacity(minCapacity);
        return ret;
    }

    public int getCapacity() {
        return this.capacity;
    }

    @Override
    public boolean addAll(final Collection<? extends ByteBuffer> c) {
        throw reject("addAll");
    }

    @Override
    public boolean addAll(final int index, final Collection<? extends ByteBuffer> c) {
        throw reject("addAll");
    }

    @Override
    public boolean add(final ByteBuffer element) {
        if (element == null) {
            throw new NullPointerException("element");
        }
        this.capacity += element.remaining();
        return super.add(element);
    }

    @Override
    public void add(final int index, final ByteBuffer element) {
        if (element == null) {
            throw new NullPointerException("element");
        }
        this.capacity += element.remaining();
        super.add(index, element);
    }

    @Override
    public ByteBuffer set(final int index, final ByteBuffer element) {
        throw reject("set");
    }

    @Override
    public ByteBuffer remove(final int index) {
        throw reject("remove");
    }

    @Override
    public boolean remove(final Object o) {
        throw reject("remove");
    }

    @Override
    public boolean recycle() {
        clear();
        this.capacity = 0;
        return recyclers.recycle(this, handle);
    }

    public static int threadLocalCapacity() {
        return recyclers.threadLocalCapacity();
    }

    public static int threadLocalSize() {
        return recyclers.threadLocalSize();
    }

    private static UnsupportedOperationException reject(final String message) {
        return new UnsupportedOperationException(message);
    }

    private RecyclableByteBufferList(final Recyclers.Handle handle) {
        this(handle, DEFAULT_INITIAL_CAPACITY);
    }

    private RecyclableByteBufferList(final Recyclers.Handle handle, final int initialCapacity) {
        super(initialCapacity);
        this.handle = handle;
    }

    private final transient Recyclers.Handle handle;

    private static final Recyclers<RecyclableByteBufferList> recyclers
        = new Recyclers<RecyclableByteBufferList>(512) {
        @Override
        protected RecyclableByteBufferList newObject(final Handle handle) {
            return new RecyclableByteBufferList(handle);
        }
    };
}
