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

package io.dingodb.test.exception;

import io.dingodb.test.SqlHelper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Slf4j
public class PreparedExceptionTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @Test
    public void testExceptionWithParameters() {
        String sql = "select ?";
        SQLException e = assertThrows(SQLException.class, () -> {
            try (PreparedStatement statement = sqlHelper.getConnection().prepareStatement(sql)) {
                statement.execute();
            }
        });
        assertThat(e.getErrorCode()).isEqualTo(54001);
        assertThat(e.getSQLState()).isEqualTo("54001");
    }
}
