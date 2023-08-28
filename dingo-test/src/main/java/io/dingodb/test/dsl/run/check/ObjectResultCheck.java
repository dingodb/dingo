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

package io.dingodb.test.dsl.run.check;

import io.dingodb.test.asserts.Assert;
import io.dingodb.test.asserts.ResultSetCheckConfig;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
public final class ObjectResultCheck extends ResultCheck {
    private final String[] columnLabels;
    private final List<Object[]> tuples;
    private final ResultSetCheckConfig config;

    @Override
    public void checkResultSet(
        @NonNull ResultSet resultSet,
        @NonNull CheckContext context
    ) throws SQLException {
        Assert.resultSet(resultSet).config(config)
            .columnLabels(columnLabels)
            .isRecords(tuples);
        log.debug("[PASSED] checking for column labels and rows in result set: {}", context.getInfo());
    }
}
