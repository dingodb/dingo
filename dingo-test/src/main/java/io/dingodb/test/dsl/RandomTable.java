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

package io.dingodb.test.dsl;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.UUID;
import javax.annotation.Nonnull;

public class RandomTable {
    private static final String TABLE_NAME_PLACEHOLDER = "table";

    private final String name;
    private final int index;

    public RandomTable() {
        this(0);
    }

    public RandomTable(int index) {
        this.name = "tbl_" + UUID.randomUUID().toString().replace('-', '_');
        this.index = index;
    }

    @Override
    public @Nonnull String toString() {
        return getName();
    }

    public @Nonnull String getName() {
        return name + (index > 0 ? "_" + index : "");
    }

    private @Nonnull String getPlaceholder() {
        return "{" + TABLE_NAME_PLACEHOLDER + (index > 0 ? "_" + index : "") + "}";
    }

    public @NonNull String transSql(@NonNull String sql) {
        return sql.replace(getPlaceholder(), getName());
    }
}
