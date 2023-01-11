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

import io.dingodb.mpu.instruction.Instruction;

public interface Writer extends AutoCloseable {

    /**
     * Returns the number of updates in the batch.
     *
     * @return number of items in WriteBatch
     */
    int count();

    /**
     * Returns the instruction corresponding to this writer.
     *
     * @return instruction
     */
    Instruction instruction();

    /**
     * <p>Set the mapping "key-value" in the database.</p>
     *
     * @param key the specified key to be inserted.
     * @param value the value associated with the specified key.
     */
    void set(byte[] key, byte[] value);

    /**
     * <p>If contains a mapping for "key", erase it.  Else do nothing.</p>
     *
     * @param key Key to delete within database
     */
    void erase(byte[] key);

    /**
     * Erase the data in the range ["begin", "end"), i.e.,
     * including "begin" and excluding "end".
     *
     * @param begin
     *          First key to delete within database (included)
     * @param end
     *          Last key to delete within database (excluded)
     */
    void erase(byte[] begin, byte[] end);

    @Override
    void close();

}
