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

package io.dingodb.test.join;

import io.dingodb.test.SqlHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Objects;

@Slf4j
public class QueryHashJoinTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
        sqlHelper.execFile(QueryHashJoinTest.class.getResourceAsStream("table-artists-create.sql"));
        sqlHelper.execFile(QueryHashJoinTest.class.getResourceAsStream("table-songs-create.sql"));
        sqlHelper.execFile(QueryHashJoinTest.class.getResourceAsStream("table-artists-data.sql"));
        sqlHelper.execFile(QueryHashJoinTest.class.getResourceAsStream("table-songs-data.sql"));
    }

    @AfterAll
    public static void cleanUpAll() throws Exception {
        sqlHelper.cleanUp();
    }

    @BeforeEach
    public void setup() throws Exception {
    }

    @AfterEach
    public void cleanUp() throws Exception {
    }

    private void testJoin(String fileName) throws SQLException, IOException {
        String sql = IOUtils.toString(
            Objects.requireNonNull(this.getClass().getResourceAsStream(fileName + ".sql")),
            StandardCharsets.UTF_8
        );
        sqlHelper.queryTest(sql, this.getClass().getResourceAsStream(fileName + ".csv"));
    }

    @Test
    public void testSongsArtistsInner() throws SQLException, IOException {
        testJoin("songs-artists-inner-join");
    }

    @Test
    public void testSongsArtistsLeft() throws SQLException, IOException {
        testJoin("songs-artists-left-join");
    }

    @Test
    public void testSongsArtistsRight() throws SQLException, IOException {
        testJoin("songs-artists-right-join");
    }

    @Test
    public void testSongsArtistsFull() throws SQLException, IOException {
        testJoin("songs-artists-full-join");
    }
}
