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

public class ColumnBlock extends ColumnObject {
    public ColumnBlock() {
        super(newColumnBlock());
    }

    public void test(long count) {
        System.out.println("java test: " + test(nativeHandle_, count));
    }

    public void clearBlock() {
        clearBlock(nativeHandle_);
    }

    // write
    public int appendData(String database, String table, String columnName, final String[] columnData) {
        return appendData(nativeHandle_, database, table, columnName, columnData);
    }

    public int blockInsertTable(final String sessionID, final String database, final String table) {
        return blockInsertTable(nativeHandle_, sessionID, database, table);
    }

    public int blockSelectTableBegin(final String sessionID,
                                     final String database,
                                     final String table,
                                     final String primaryKey,
                                     final String[] columnNames) {
        return this.blockSelectTableBegin(sessionID,
                database,
                table,
                primaryKey,
                columnNames,
                0);
    }

    public int blockSelectTableBegin(final String sessionID,
                                     final String database,
                                     final String table,
                                     final String primaryKey,
                                     final String[] columnNames,
                                     final int limit) {
        return this.blockSelectTableBegin(
                sessionID,
                database,
                table,
                primaryKey,
                columnNames,
                null,
                null,
                null,
                limit);
    }

    public int blockSelectTableBegin(final String sessionID,
                                     final String database,
                                     final String table,
                                     final String primaryKey,
                                     final String[] columnNames,
                                     final String pointLookupValue) {
        return this.blockSelectTableBegin(
                sessionID,
                database,
                table,
                primaryKey,
                columnNames,
                pointLookupValue,
                0);
    }

    public int blockSelectTableBegin(final String sessionID,
                                     final String database,
                                     final String table,
                                     final String primaryKey,
                                     final String[] columnNames,
                                     final String pointLookupValue,
                                     final int limit) {
        return this.blockSelectTableBegin(sessionID,
                database,
                table,
                primaryKey,
                columnNames,
                pointLookupValue,
                null,
                null,
                limit);
    }

    public int blockSelectTableBegin(final String sessionID,
                                     final String database,
                                     final String table,
                                     final String primaryKey,
                                     final String[] columnNames,
                                     final String begin,
                                     final String end) {
        return this.blockSelectTableBegin(sessionID,
                database,
                table,
                primaryKey,
                columnNames,
                begin,
                end,
                0);
    }

    public int blockSelectTableBegin(final String sessionID,
                                     final String database,
                                     final String table,
                                     final String primaryKey,
                                     final String[] columnNames,
                                     final String begin,
                                     final String end,
                                     final int limit) {
        return this.blockSelectTableBegin(sessionID,
                database,
                table,
                primaryKey,
                columnNames,
                null,
                begin,
                end,
                limit);
    }

    public int blockSelectTableNext(final String sessionID) {
        return blockSelectTableNext(nativeHandle_, sessionID);
    }

    public int blockSelectTableCancel(final String sessionID) {
        return blockSelectTableCancel(nativeHandle_, sessionID);
    }

    private int blockSelectTableBegin(final String sessionID,
                                      final String database,
                                      final String table,
                                      final String primaryKey,
                                      final String[] columnNames,
                                      final String pointLookupValue,
                                      final String begin,
                                      final String end,
                                      final int limit) {
        return blockSelectTableBegin(nativeHandle_,
                sessionID,
                database,
                table,
                primaryKey,
                columnNames,
                pointLookupValue,
                begin,
                end,
                limit);
    }

    // read
    public long getRows() {
        return getRows(nativeHandle_);
    }

    public long getColumns() {
        return getColumns(nativeHandle_);
    }

    public String[] getColumnData(long index, int type) {
        return getColumnData(nativeHandle_, index, type);
    }

    private static native long newColumnBlock();

    @Override protected final native void disposeInternal(final long handle);

    private native String test(final long handle, final long count);

    private native void clearBlock(final long handle);

    private native int appendData(final long handle, final String database, final String table, final String columnName,
                                  final String[] columnData);

    private native int blockInsertTable(final long handle, final String sessionID, final String database,
                                        final String table);

    private native int blockSelectTableBegin(final long handle,
                                             final String sessionID,
                                             final String database,
                                             final String table,
                                             final String primaryKey,
                                             final String[] columnNames,
                                             final String pointLookupValue,
                                             final String begin,
                                             final String end,
                                             int limit);

    private native int blockSelectTableNext(final long handle, final String sessionID);

    private native int blockSelectTableCancel(final long handle, final String sessionID);

    private native long getRows(final long handle);

    private native long getColumns(final long handle);

    private native String[] getColumnData(final long handle, long index, int type);
}
