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

import io.dingodb.common.table.TupleSchema;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

@Slf4j
public class QueryHashJoinTest {
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        sqlHelper = new SqlHelper();
        sqlHelper.execFile("/table-artists-create.sql");
        sqlHelper.execFile("/table-songs-create.sql");
        sqlHelper.execFile("/table-artists-data.sql");
        sqlHelper.execFile("/table-songs-data.sql");
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

    @Test
    public void testInnerJoin() throws SQLException, IOException {
        String sql = "select s.id, s.title, a.name as artistName, a.category as artistCategory"
            + " from songs s join artists a on s.artist = a.id";
        sqlHelper.queryTest(
            sql,
            new String[]{"id", "title", "artistName", "artistCategory"},
            TupleSchema.ofTypes("INTEGER", "STRING", "STRING", "STRING"),
            "1, My Heart Will Go On, Celine Dion, Female\n"
                + "2, The Power Of Love, Celine Dion, Female\n"
                + "3, Saving All My Love For You, Whitney Houston, Female\n"
                + "4, I'm Your Baby Tonight, Whitney Houston, Female\n"
                + "5, Miracle, Whitney Houston, Female\n"
                + "6, Swear It Again, Westlife, Band\n"
                + "7, Crazy For You, Madonna, Female\n"
                + "8, I Want It That Way, BackStreet Boys, Band\n"
                + "9, As Long As You Love Me, BackStreet Boys, Band\n"
                + "10, This Place Hotel, Michael Jackson, Male\n"
                + "11, Rolling In The Deep, Adele Adkins, Female\n"
                + "12, Right Here Waiting, Richard Marx, Male\n"
                + "13, What The Hell, Avril Lavigne, Female\n"
                + "14, Goodbye, Avril Lavigne, Female\n"
                + "15, Remember When, Avril Lavigne, Female\n"
                + "16, Trip To Your Heart, Britney Spears, Female\n"
                + "17, Trouble For Me, Britney Spears, Female"
        );
    }
}
