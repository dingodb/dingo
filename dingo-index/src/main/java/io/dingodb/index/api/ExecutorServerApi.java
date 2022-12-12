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

package io.dingodb.index.api;

import io.dingodb.common.store.KeyValue;

import java.util.List;

public interface ExecutorServerApi {

    int tableDefinitionVersion = 0;

    boolean updateTableDefinitionVersion(int version);
    List<KeyValue> getAllFinishedRecord();
    List<KeyValue> getAllUnfinishedRecord();
    boolean insertUnfinishedRecord(KeyValue record);
    boolean deleteUnfinishedRecord(byte[] key);
    default boolean insertFinishedRecord(byte[] key, int tableDefinitionVersion) {
        byte[] unfinishedKey = getUnfinishedKey(key);
        if (unfinishedKey != null) {
            if (tableDefinitionVersion == tableDefinitionVersion) {
                // insert (flag + key)
                // delete unfinishedKey
            } else {
                //throw exception
            }
        } else {
            byte[] finishedKey = getFinishedKey(key);
            if (finishedKey == null) {
                //throw exception
            }
        }
        return true;
    }

    byte[] getUnfinishedKey(byte[] key);
    byte[] getFinishedKey(byte[] key);
    boolean insertIndex(KeyValue record);

    KeyValue getRecord(byte[] key);

    void deleteIndex(KeyValue keyValue);

    void deleteFinishedRecord(byte[] key);

    List<KeyValue> getFinishedRecord(List<KeyValue> records);

    void insertDeleteKey(KeyValue keyValue);

    void deleteDeleteKey(byte[] key);

    List<KeyValue> getAllDeleteRecord();
}
