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

package io.dingodb.test;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import javax.annotation.Nonnull;

@Slf4j
@RequiredArgsConstructor
public final class SqlHelper {
    private final Connection connection;

    @SuppressWarnings("UnusedReturnValue")
    public int execUpdate(@Nonnull String sqlFile) throws IOException, SQLException {
        int result = -1;
        String[] sqlList = IOUtils.toString(
            Objects.requireNonNull(SqlHelper.class.getResourceAsStream(sqlFile)),
            StandardCharsets.UTF_8
        ).split(";");
        try (Statement statement = connection.createStatement()) {
            for (String sql : sqlList) {
                if (!sql.trim().isEmpty()) {
                    result = statement.executeUpdate(sql);
                }
            }
        }
        return result;
    }

    public void clear(String tableName) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("delete from " + tableName);
        }
    }
}
