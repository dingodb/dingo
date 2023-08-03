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

package io.dingodb.calcite.operation;

import io.dingodb.codec.KeyValueCodec;
import io.dingodb.store.api.StoreInstance;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public interface Operation {

    default Boolean internalExecute(Connection connection, String sql) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            return statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    default void insert(StoreInstance store, KeyValueCodec codec, List<Object[]> rowList) {
        rowList.stream().forEach(row -> {
            try {
                store.insert(codec.encode(row));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    default void insert(StoreInstance store, KeyValueCodec codec, Object[] row) {
        try {
            store.insert(codec.encode(row));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    default void deletePrefix(StoreInstance store, KeyValueCodec codec, Object[] key) {
        try {
            byte[] prefix = codec.encodeKeyPrefix(key, 2);
            store.delete(new StoreInstance.Range(prefix, prefix, true, true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
