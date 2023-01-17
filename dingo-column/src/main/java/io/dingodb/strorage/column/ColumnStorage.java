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

package io.dingodb.strorage.column;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
//import org.rocksdb.util.Environment;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ColumnStorage extends ColumnObject {
    public static final int NOT_FOUND = -1;

    private enum LibraryState {
        NOT_LOADED,
        LOADING,
        LOADED
    }

    private static final AtomicReference<LibraryState> libraryLoaded =
        new AtomicReference<>(LibraryState.NOT_LOADED);

    public static void loadLibrary() {
        if (libraryLoaded.get() == LibraryState.LOADED) {
            return;
        }

        if (libraryLoaded.compareAndSet(LibraryState.NOT_LOADED,
            LibraryState.LOADING)) {
            try {
                System.loadLibrary("dingo_merge_tree"); // libdingo_merge_tree.so
            } catch (final Exception e) {
                libraryLoaded.set(LibraryState.NOT_LOADED);
                throw new RuntimeException("Unable to load the ColumnStorage shared library",
                    e);
            }

            libraryLoaded.set(LibraryState.LOADED);
            return;
        }

        while (libraryLoaded.get() == LibraryState.LOADING) {
            try {
                Thread.sleep(10);
            } catch(final InterruptedException e) {
                //ignore
            }
        }
    }

    /**
     * Private constructor.
     *
     * The native handle of the C++ Column object
     */
    public ColumnStorage() {
        super(newStorage());
    }

    @Override
    public void close() {
        if (owningHandle_.compareAndSet(true, false)) {
            if (owningHandle_.compareAndSet(true, false)) {
                disposeInternal();
            }
        }
    }

    public int columnTest(final String para1, final String para2) {
        return columnTest(nativeHandle_, para1, para2);
    }

    public int columnInit(final String dataDir, final String logDir) {
        return columnInit(nativeHandle_, dataDir, logDir);
    }

    public void columnDestroy()
    {
        columnDestroy(nativeHandle_);
    }

    public int createTable(final String sessionID, final String database, final String table, final String engine,
                           final String[] columnNames, final int[] columnTypes) {
        return createTable(nativeHandle_, sessionID, database, table, engine, columnNames, columnTypes);
    }

    public int mergeTable(final String sessionID, final String database, final String table, final String partition) {
        return mergeTable(nativeHandle_, sessionID, database, table, partition);
    }

    public int dropTable(final String sessionID, final String database, final String table) {
        return dropTable(nativeHandle_, sessionID, database, table);
    }

    @Override protected native void disposeInternal(final long handle);

    private static native long newStorage();

    private native int columnTest(final long handle, final String para1, final String para2);

    private native int columnInit(final long handle, final String dataDir, final String logDir);

    private native void columnDestroy(final long handle);

    private native int createTable(final long handle, final String sessionID, final String database, final String table,
                                   final String engine, final String[] columnNames, final int[] columnTypes);

    private native int mergeTable(final long handle, final String sessionID, final String database, final String table,
                                  final String partition);

    private native int dropTable(final long handle, final String sessionID, final String database, final String table);
}
