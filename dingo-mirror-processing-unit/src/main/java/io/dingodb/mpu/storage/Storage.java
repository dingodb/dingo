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

package io.dingodb.mpu.storage;

import io.dingodb.mpu.core.CoreMeta;
import io.dingodb.mpu.instruction.Context;
import io.dingodb.mpu.instruction.Instruction;

import java.util.concurrent.CompletableFuture;

public interface Storage {

    long clocked();

    long clock();

    void tick(long clock);

    void saveInstruction(long clock, byte[] instruction);

    byte[] reappearInstruction(long clock);

    void destroy();

    CompletableFuture<Void> transferTo(CoreMeta meta);

    String filePath();

    String receiveBackup();

    void applyBackup();

    void clearClock(long clock);

    long approximateCount();

    long approximateSize();

    Reader metaReader();

    Writer metaWriter(Instruction instruction);

    /**
     * Get storage snapshot reader.
     *
     * @return reader
     */
    Reader reader();

    /**
     * Create writer for instruction.
     * @return writer
     */
    Writer writer(Instruction instruction);

    /**
     * Flush data from writer to storage.
     *
     * @param writer writer
     */
    void flush(Writer writer);

    /**
     * Flush data from writer to storage.
     *
     * @param context context
     */
    default void flush(Context context) {
        flush(context.writer());
    }

}
