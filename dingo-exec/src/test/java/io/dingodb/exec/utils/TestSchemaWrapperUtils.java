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

package io.dingodb.exec.utils;

import com.google.common.collect.ImmutableList;
import io.dingodb.common.SchemaWrapper;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.TupleMapping;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSchemaWrapperUtils {
    @Test
    public void testBuildSchemaWrapper() {
        DingoType type = DingoTypeFactory.INSTANCE.tuple("INT", "LONG", "STRING", "DOUBLE|NULL");
        TupleMapping mapping = TupleMapping.of(ImmutableList.of(0, 2));
        SchemaWrapper sw = SchemaWrapperUtils.buildSchemaWrapper(type, mapping, 1);
        assertThat(sw.getCommonId()).isEqualTo(1);
        List<ColumnDefinition> columns = sw.getSchemas();
        assertThat(columns).hasSize(4);
        assertThat(columns).element(0)
            .hasFieldOrPropertyWithValue("typeName", "INT")
            .hasFieldOrPropertyWithValue("nullable", false)
            .hasFieldOrPropertyWithValue("primary", 0);
        assertThat(columns).element(1)
            .hasFieldOrPropertyWithValue("typeName", "LONG")
            .hasFieldOrPropertyWithValue("nullable", false)
            .hasFieldOrPropertyWithValue("primary", -1);
        assertThat(columns).element(2)
            .hasFieldOrPropertyWithValue("typeName", "STRING")
            .hasFieldOrPropertyWithValue("nullable", false)
            .hasFieldOrPropertyWithValue("primary", 1);
        assertThat(columns).element(3)
            .hasFieldOrPropertyWithValue("typeName", "DOUBLE")
            .hasFieldOrPropertyWithValue("nullable", true)
            .hasFieldOrPropertyWithValue("primary", -1);
    }
}
