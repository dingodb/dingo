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

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
public final class UpdateCountCheck implements Check {
    private final int updateCount;

    @Override
    public void check(@NonNull CheckContext context) throws SQLException {
        Check.super.check(context);
        assertThat(context.getExecuteReturnedValue()).isFalse();
        assertThat(context.getStatement().getUpdateCount()).isEqualTo(updateCount);
        log.debug("[PASSED] checking for update count: {}", context.getInfo());
    }
}
